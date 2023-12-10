package core

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/andybalholm/brotli"
)

type ConnectionCreds struct {
	Host     string
	Port     int16
	Database string
	Username string
	Password string
	SSL      bool
}

type Options struct {
	DataOnly   bool
	Compress   bool
	RecordMode string
}

type Manager struct {
	OutputFilename *string
	Database       *sql.DB
	Options        *Options
}

func NewManager(outputFile *string, connData *ConnectionCreds, options *Options) (Manager, error) {
	sslMode := "disable"
	if connData.SSL {
		sslMode = "enable"
	}

	db, err := sql.Open("postgres", fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s&TimeZone=UTC", connData.Username, connData.Password, connData.Host, connData.Port, connData.Database, sslMode))
	if err != nil {
		return Manager{}, fmt.Errorf("error while connecting to the database: %v", err)
	}

	return Manager{outputFile, db, options}, nil
}

func (m Manager) Compress() {
	// Open files
	inFile, _ := os.Open("dump.sql")
	defer inFile.Close()
	outFile, _ := os.Create("dump.sql.br")
	defer outFile.Close()

	// Create brotli writer
	writer := brotli.NewWriterLevel(outFile, brotli.BestCompression)
	defer writer.Close()

	// Copy & compress input file to output file
	_, err := io.Copy(writer, inFile)
	if err != nil {
		panic(err)
	}

	println("File compressed successfully")
}

func (m Manager) getLockFilename() string {
	return fmt.Sprintf("%s.lock", *m.OutputFilename)
}

func (m Manager) init() error {
	recordMode := strings.ToLower(m.Options.RecordMode)
	if !(recordMode == "insert" || recordMode == "copy") {
		return fmt.Errorf("invalid value for record mode. Use either 'INSERT' or 'COPY'.")
	}
	m.Options.RecordMode = recordMode

	if _, err := os.Stat(*m.OutputFilename); err == nil {
		fmt.Print("the specified output file already exists. Overwrite [y/N]? ")

		reader := bufio.NewReader(os.Stdin)
		response, _ := reader.ReadString('\n')
		response = strings.TrimSpace(strings.ToLower(response))

		if response != "y" {
			fmt.Println("Aborting...")
			os.Exit(0)
		}
	}

	var d []byte
	if err := os.WriteFile(*m.OutputFilename, d, 0644); err != nil {
		return fmt.Errorf("cannot write to output file: %v", err)
	}

	if err := os.WriteFile(m.getLockFilename(), d, 0644); err != nil {
		return fmt.Errorf("cannot create lock file: %v", err)
	}

	// TODO: Definitely more checks are needed but it's 1 AM and
	//		 I've been studying non-stop for the 2 days. I've had enough for tonight.

	return nil
}

func (m Manager) cleanup() error {
	if err := os.Remove(m.getLockFilename()); err != nil {
		return fmt.Errorf("cannot remove lock file: %v", err)
	}

	return nil
}

func (m Manager) Pack() error {
	if err := m.init(); err != nil {
		return fmt.Errorf("cannot initialize pack job: %v", err)
	}

	defer m.cleanup()

	// Open the output file
	outputFile, err := os.Create(*m.OutputFilename)
	if err != nil {
		return fmt.Errorf("error while creating output file: %v", err)
	}

	// Get the list of tables in the database
	tables, err := m.getTables()
	if err != nil {
		return fmt.Errorf("error while fetching tables: %v", err)
	}

	// Drop tables
	_, err = outputFile.WriteString("\n--\n-- START OF DROPPING TABLES\n--\n")
	for _, table := range tables {
		dropTableStmt := fmt.Sprintf("DROP TABLE IF EXISTS %s;", table)

		_, err = outputFile.WriteString(dropTableStmt + "\n")
		if err != nil {
			return fmt.Errorf("error while writing DROP statement: %v", err)
		}
	}
	_, err = outputFile.WriteString("\n--\n-- END OF DROPPING TABLES\n--\n")

	// Create tables
	_, err = outputFile.WriteString("\n--\n-- START OF CREATING TABLES\n--\n")
	for _, table := range tables {
		createTableStmt, err := m.getCreateTableStatement(table)
		if err != nil {
			return fmt.Errorf("error while composing CREATE statement: %v", err)
		}
		_, err = outputFile.WriteString(createTableStmt + "\n\n")
		if err != nil {
			return fmt.Errorf("error while writing CREATE statement: %v", err)
		}
	}
	_, err = outputFile.WriteString("\n--\n-- END OF CREATING TABLES\n--\n")

	// Add records
	_, err = outputFile.WriteString("\n--\n-- START OF RECORDS\n--\n")
	for _, table := range tables {
		_, err = outputFile.WriteString("\n---\n--- Table: " + table + "\n---\n")

		ch := make(chan string)
		switch m.Options.RecordMode {
		case "insert":
			err = m.broadcastTableRecordsINSERT(table, ch)
		case "copy":
			err = m.broadcastTableRecordsCOPY(table, ch)
		}

		if err != nil {
			return fmt.Errorf("error while receiving data records: %v", err)
		}

		for record := range ch {
			_, err = outputFile.WriteString(record)
			if err != nil {
				return fmt.Errorf("error while writing data records: %v", err)
			}
		}
	}
	outputFile.WriteString("\n--\n-- END OF RECORDS\n--\n")

	// Constraints
	_, err = outputFile.WriteString("\n--\n-- START OF CONSTRAINTS\n--\n")
	for _, table := range tables {
		outputFile.WriteString("\n---\n--- Constraint: PRIMARY KEY\n--- Table: " + table + "\n---\n")

		pkStmt, err := m.getPrimaryKeyStatements(table)
		if err != nil {
			return err
		}

		_, err = outputFile.WriteString(pkStmt + "\n")
		if err != nil {
			return err
		}
	}
	for _, table := range tables {
		_, err = outputFile.WriteString("\n---\n--- Constraint: FOREIGN KEY\n--- Table: " + table + "\n---\n")
		fkStmt, err := m.getForeignKeyStatements(table)
		if err != nil {
			return err
		}

		_, err = outputFile.WriteString(fkStmt + "\n")
		if err != nil {
			return err
		}
	}
	_, err = outputFile.WriteString("\n--\n-- END OF CONSTRAINTS\n--\n")

	outputFile.Close()

	// Compress
	if m.Options.Compress {
		fileNameSegments := strings.Split(*m.OutputFilename, ".")
		compOutFilename := fmt.Sprintf("%s.pack", fileNameSegments[0:len(fileNameSegments)-1])
		compOutFile, _ := os.Create(compOutFilename)
		defer compOutFile.Close()

		outputFile, err := os.Open(*m.OutputFilename)
		if err != nil {
			log.Fatal(err)
		}

		// Create brotli writer
		writer := brotli.NewWriterLevel(compOutFile, brotli.BestCompression)
		defer writer.Close()

		// Copy & compress input file to output file
		_, err = io.Copy(writer, outputFile)
		if err != nil {
			return fmt.Errorf("error while compressing pack file: %v", err)
		}

		outputFile.Close()

		if err = os.Remove(outputFile.Name()); err != nil {
			return fmt.Errorf("error while deleting plain pack file: %v", err)
		}
	}

	return nil
}

func (m Manager) getTables() ([]string, error) {
	rows, err := m.Database.Query("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		if err != nil {
			return nil, err
		}
		tables = append(tables, tableName)
	}

	return tables, nil
}

func (m Manager) getCreateTableStatement(tableName string) (string, error) {
	rows, err := m.Database.Query(fmt.Sprintf("SELECT column_name, data_type, is_nullable, column_default, character_maximum_length, numeric_precision, numeric_scale FROM information_schema.columns WHERE table_name = '%s' ORDER BY ordinal_position", tableName))
	if err != nil {
		return "", err
	}
	defer rows.Close()

	columnDefs := make([]string, 0)
	for rows.Next() {
		var columnName, dataType, isNullable, columnDefault sql.NullString
		var characterMaximumLength, numericPrecision, numericScale sql.NullInt64
		err := rows.Scan(&columnName, &dataType, &isNullable, &columnDefault, &characterMaximumLength, &numericPrecision, &numericScale)
		if err != nil {
			return "", err
		}

		if !(columnName.Valid && dataType.Valid) {
			continue
		}
		columnDef := columnName.String + " " + dataType.String

		precisionTypes := map[string]bool{
			"numeric":   true,
			"decimal":   true,
			"timestamp": true,
			"interval":  true,
		}

		if characterMaximumLength.Valid {
			columnDef += fmt.Sprintf("(%d)", characterMaximumLength.Int64)
		} else if precisionTypes[dataType.String] && numericPrecision.Valid {
			if numericScale.Valid {
				columnDef += fmt.Sprintf("(%d,%d)", numericPrecision.Int64, numericScale.Int64)
			} else {
				columnDef += fmt.Sprintf("(%d)", numericPrecision.Int64)
			}
		}

		if isNullable.String == "NO" {
			columnDef += " NOT NULL"
		}

		if columnDefault.Valid {
			columnDef += " DEFAULT " + columnDefault.String
		}

		columnDefs = append(columnDefs, columnDef)
	}

	if len(columnDefs) == 0 {
		return "", fmt.Errorf("Table '%s' not found", tableName)
	}

	createTableStmt := fmt.Sprintf("CREATE TABLE %s (\n\t%s\n);", tableName, strings.Join(columnDefs, ",\n\t"))

	return createTableStmt, nil
}

func (m Manager) getPrimaryKeyStatements(tableName string) (string, error) {
	var statements []string

	// Get primary keys
	rows, err := m.Database.Query(`
		SELECT
			kcu.column_name,
			tc.constraint_name
		FROM information_schema.table_constraints AS tc 
		JOIN information_schema.key_column_usage AS kcu
			ON tc.constraint_name = kcu.constraint_name
			AND tc.table_schema = kcu.table_schema
		WHERE tc.constraint_type = 'PRIMARY KEY'
			AND tc.table_schema='public'
			AND tc.table_name=$1;	
	`, tableName)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var pks []string
	var constraintName string
	for rows.Next() {
		var pk string
		err := rows.Scan(&pk, &constraintName)
		if err != nil {
			return "", err
		}
		pks = append(pks, pk)
	}

	if len(pks) > 0 {
		statements = append(statements, fmt.Sprintf("ALTER TABLE ONLY %s\n\tADD CONSTRAINT %s PRIMARY KEY (%s);", tableName, constraintName, strings.Join(pks, ", ")))
	}

	return strings.Join(statements, "\n"), nil
}

func (m Manager) getForeignKeyStatements(tableName string) (string, error) {
	var statements []string

	rows, err := m.Database.Query(`
		SELECT
			tc.table_schema, 
			tc.constraint_name, 
			tc.table_name, 
			kcu.column_name, 
			ccu.table_schema AS foreign_table_schema,
			ccu.table_name AS foreign_table_name,
			ccu.column_name AS foreign_column_name 
		FROM information_schema.table_constraints AS tc 
		JOIN information_schema.key_column_usage AS kcu
			ON tc.constraint_name = kcu.constraint_name
			AND tc.table_schema = kcu.table_schema
		JOIN information_schema.constraint_column_usage AS ccu
			ON ccu.constraint_name = tc.constraint_name
		WHERE tc.constraint_type = 'FOREIGN KEY'
			AND tc.table_schema='public'
			AND tc.table_name=$1;
	`, tableName)
	if err != nil {
		return "", err
	}

	for rows.Next() {
		var tableSchema, constraintName, _tableName, columnName, foreignTableSchema, foreignTableName, foreignColumnName string
		err := rows.Scan(&tableSchema, &constraintName, &_tableName, &columnName, &foreignTableSchema, &foreignTableName, &foreignColumnName)
		if err != nil {
			return "", err
		}
		statements = append(statements, fmt.Sprintf("ALTER TABLE ONLY %s.%s\n\tADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s.%s(%s);",
			tableSchema, _tableName, constraintName, columnName, foreignTableSchema, foreignTableName, foreignColumnName))
	}

	return strings.Join(statements, "\n"), nil
}

func (m Manager) broadcastTableRecordsINSERT(tableName string, ch chan string) error {
	go func() {
		selectDataSQL := fmt.Sprintf("SELECT * FROM %s", tableName)
		rows, err := m.Database.Query(selectDataSQL)
		if err != nil {
			// return fmt.Errorf("error while fetching data: %v", err)
			return
		}

		defer rows.Close()
		defer close(ch)

		columnNames, err := rows.Columns()
		if err != nil {
			// return fmt.Errorf("error while fetching columns: %v", err)
			return
		}

		values := make([]interface{}, len(columnNames))
		valuePointers := make([]interface{}, len(columnNames))
		for i := range columnNames {
			valuePointers[i] = &values[i]
		}

		for rows.Next() {
			err := rows.Scan(valuePointers...)
			if err != nil {
				return
			}

			insertStmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (",
				tableName, strings.Join(columnNames, ", "))

			for i, value := range values {
				if value == nil {
					insertStmt += "NULL"
				} else {
					strValue := fmt.Sprintf("%v", value)
					switch value.(type) {
					case int, int64, float64:
						insertStmt += strValue
					case bool:
						if value.(bool) {
							insertStmt += "TRUE"
						} else {
							insertStmt += "FALSE"
						}
					case string:
						insertStmt += fmt.Sprintf("'%v'", strings.ReplaceAll(strValue, "'", "''"))
					case time.Time:
						t, err := time.Parse("2006-01-02 15:04:05 -0700 -0700", strValue)
						// t, err := value.(time.Time)
						if err != nil {
							continue
						}

						h, m, d := t.Clock()
						// TODO: This is a faulty way of checking whether 't' is of type 'time' or 'date'
						if h+m+d != 0 {
							// (likely) time
							insertStmt += fmt.Sprintf("'%s'", strValue)
						} else {
							// date
							insertStmt += fmt.Sprintf("'%s'", t.Format("2006-01-02"))
						}
					default:
						insertStmt += fmt.Sprintf("'%s'", strValue)
					}
				}

				if i != len(values)-1 {
					insertStmt += ", "
				}
			}

			insertStmt += ");\n"
			ch <- insertStmt
		}

	}()
	return nil
}

func (m Manager) broadcastTableRecordsCOPY(tableName string, ch chan string) error {
	go func() {
		selectDataSQL := fmt.Sprintf("SELECT * FROM %s", tableName)
		rows, err := m.Database.Query(selectDataSQL)
		if err != nil {
			return
		}
		defer rows.Close()
		defer close(ch)

		columnNames, err := rows.Columns()
		if err != nil {
			return
		}

		values := make([]interface{}, len(columnNames))
		valuePointers := make([]interface{}, len(columnNames))
		for i := range columnNames {
			valuePointers[i] = &values[i]
		}

		ch <- fmt.Sprintf("COPY %s (%s) FROM stdin;\n",
			tableName, strings.Join(columnNames, ", "))

		for rows.Next() {
			err := rows.Scan(valuePointers...)
			if err != nil {
				return
			}

			valueParams := []string{}

			for _, value := range values {
				if value == nil {
					valueParams = append(valueParams, "NULL")
				} else {
					strValue := fmt.Sprintf("%v", value)
					switch value.(type) {
					case int, int64, float64:
						valueParams = append(valueParams, strValue)
					case bool:
						if value.(bool) {
							valueParams = append(valueParams, "TRUE")
						} else {
							valueParams = append(valueParams, "FALSE")
						}
					case string:
						valueParams = append(valueParams, fmt.Sprintf("'%v'", strings.ReplaceAll(strValue, "'", "''")))
					case time.Time:
						t, err := time.Parse("2006-01-02 15:04:05 -0700 -0700", strValue)
						// t, err := value.(time.Time)
						if err != nil {
							continue
						}

						h, m, d := t.Clock()
						// TODO: This is a faulty way of checking whether 't' is of type 'time' or 'date'
						if h+m+d != 0 {
							// (likely) time
							valueParams = append(valueParams, fmt.Sprintf("'%s'", strValue))
						} else {
							// date
							valueParams = append(valueParams, fmt.Sprintf("'%s'", t.Format("2006-01-02")))
						}
					default:
						valueParams = append(valueParams, fmt.Sprintf("'%s'", strValue))
					}
				}

				// if i != len(values)-1 {
				// 	valueParams = append(valueParams, ", ")
				// }
			}

			ch <- strings.Join(valueParams, "\t")
			ch <- "\n"
		}

	}()
	return nil
}
