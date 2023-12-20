package core

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/andybalholm/brotli"
)

const (
	PostgresBigintMin = -9223372036854775808
	PostgresBigintMax = 9223372036854775807
)

type ConnectionCreds struct {
	Host     string
	Port     int16
	Database string
	Username string
	Password string
	SSL      bool
}

// Options contains configuration options for the packager.
// DataOnly controls whether to include schema in dump.
// Compress enables brotli compression on dump file.
// RecordMode sets the output format for table rows.
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

// NewManager creates a new Manager instance with the given output file,
// database connection credentials, and options. It opens a connection
// to the database based on the credentials, configuring SSL mode
// appropriately. Returns a Manager instance and any error from opening
// the database connection. The returned Manager is ready to perform
// packaging operations.
func NewManager(outputFile *string, connData *ConnectionCreds, options *Options) (Manager, error) {
	sslMode := "disable"
	if connData.SSL {
		sslMode = "enable"
	}

	db, err := sql.Open("postgres", fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s&TimeZone=UTC", url.QueryEscape(connData.Username), url.QueryEscape(connData.Password), connData.Host, connData.Port, connData.Database, sslMode))
	if err != nil {
		return Manager{}, fmt.Errorf("error while connecting to the database: %v", err)
	}

	return Manager{outputFile, db, options}, nil
}

// getLockFilename returns the filename to use for the lock file.
// It is based on the output filename with ".lock" appended.
func (m Manager) getLockFilename() string {
	return fmt.Sprintf("%s.lock", *m.OutputFilename)
}

// init initializes a pack job by validating options, checking for output file existence,
// creating the output file and lock file, and performing basic validation.
// It returns any error encountered during initialization.
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

// cleanup removes the lock file created during initialization.
// It is called as a deferred function after Pack() finishes.
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

	outputFile.WriteString("-- This file was created by pg_pack. DO NOT MODIFY.\n\n")

	outputFile.WriteString("SET client_encoding = 'UTF8';\n")
	outputFile.WriteString("SET statement_timeout = 0;\n")
	outputFile.WriteString("SET lock_timeout = 0;\n")
	outputFile.WriteString("SET idle_in_transaction_session_timeout = 0;\n")
	outputFile.WriteString("SET standard_conforming_strings = on;\n")
	outputFile.WriteString("SET check_function_bodies = false;\n")
	outputFile.WriteString("SET xmloption = content;\n")
	outputFile.WriteString("SET client_min_messages = warning;\n")
	outputFile.WriteString("SET row_security = off;\n")
	outputFile.WriteString("SELECT pg_catalog.set_config('search_path', '', false);\n")

	// Create tables
	schemas, err := m.getSchemas()
	if err != nil {
		return fmt.Errorf("error while fetching schemas: %v", err)
	}

	for _, schema := range schemas {
		// Get the list of tables in the database
		tables, err := m.getTables(schema)
		if err != nil {
			return fmt.Errorf("error while fetching tables: %v", err)
		}

		// Drop tables
		_, err = outputFile.WriteString("\n-- START OF DROPPING TABLES\n")
		for _, table := range tables {
			dropTableStmt := fmt.Sprintf("DROP TABLE IF EXISTS %s;", table)

			_, err = outputFile.WriteString(dropTableStmt + "\n")
			if err != nil {
				return fmt.Errorf("error while writing DROP statement: %v", err)
			}
		}
		_, err = outputFile.WriteString("-- END OF DROPPING TABLES\n")

		// Types
		_, err = outputFile.WriteString("\n-- START OF CREATING TYPES\n")
		typeStmt, err := m.getCreateTypeStatements(schema) // Only supports Enums for now
		if err != nil {
			return fmt.Errorf("error while constructing CREATE TYPE statement: %v", err)
		}
		_, err = outputFile.WriteString(typeStmt + "\n")
		_, err = outputFile.WriteString("-- END OF CREATING TYPES\n")

		// Domains
		_, err = outputFile.WriteString("\n-- START OF DOMAINS\n")
		domainStmt, err := m.getDomainStatements(schema)
		if err != nil {
			return fmt.Errorf("error while constructing DOMAIN statement: %v", err)
		}

		_, err = outputFile.WriteString(domainStmt + "\n")
		if err != nil {
			return fmt.Errorf("error while writing DOMAIN statement: %v", err)
		}
		_, err = outputFile.WriteString("-- END OF DOMAINS\n")

		// Functions
		_, err = outputFile.WriteString("\n-- START OF FUNCTIONS\n")
		functionStmt, err := m.getFunctionStatements(schema)
		if err != nil {
			return fmt.Errorf("error while constructing FUNCTION statement: %v", err)
		}

		_, err = outputFile.WriteString(functionStmt + "\n")
		if err != nil {
			return fmt.Errorf("error while writing FUNCTION statement: %v", err)
		}
		_, err = outputFile.WriteString("-- END OF FUNCTIONS\n")

		// Sequences
		_, err = outputFile.WriteString("\n-- START OF SEQUENCES\n")
		sequenceStmt, err := m.getSequenceStatements(schema)
		if err != nil {
			return fmt.Errorf("error while constructing SEQUENCE statement: %v", err)
		}

		_, err = outputFile.WriteString(sequenceStmt + "\n")
		if err != nil {
			return fmt.Errorf("error while writing SEQUENCE statement: %v", err)
		}
		_, err = outputFile.WriteString("-- END OF SEQUENCES\n")

		_, err = outputFile.WriteString("\n-- START OF CREATING TABLES\n")
		for _, table := range tables {
			createTableStmt, err := m.getCreateTableStatement(table, schema)
			if err != nil {
				return fmt.Errorf("error while constructing CREATE statement: %v", err)
			}
			_, err = outputFile.WriteString(createTableStmt + "\n\n")
			if err != nil {
				return fmt.Errorf("error while writing CREATE statement: %v", err)
			}
		}
		_, err = outputFile.WriteString("-- END OF CREATING TABLES\n")

		_, err = outputFile.WriteString("\n-- START OF RECORDS\n")
		// Add records
		for _, table := range tables {
			_, err = outputFile.WriteString("\n-- Table: " + table + "\n")

			ch := make(chan string)
			switch m.Options.RecordMode {
			case "insert":
				err = m.broadcastTableRecordsINSERT(table, schema, ch)
			case "copy":
				err = m.broadcastTableRecordsCOPY(table, schema, ch)
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
		outputFile.WriteString("-- END OF RECORDS\n")

		// Constraints
		_, err = outputFile.WriteString("\n-- START OF CONSTRAINTS\n")
		for _, table := range tables {
			outputFile.WriteString("\n-- Constraint: PRIMARY KEY\tTable: " + table + "\n")

			pkStmt, err := m.getPrimaryKeyStatements(table, schema)
			if err != nil {
				return err
			}

			_, err = outputFile.WriteString(pkStmt + "\n")
			if err != nil {
				return err
			}
		}
		for _, table := range tables {
			outputFile.WriteString("\n-- Constraint: FOREIGN KEY\tTable: " + table + "\n")
			fkStmt, err := m.getForeignKeyStatements(table, schema)
			if err != nil {
				return err
			}

			_, err = outputFile.WriteString(fkStmt + "\n")
			if err != nil {
				return err
			}
		}
		_, err = outputFile.WriteString("-- END OF CONSTRAINTS\n")
	}

	outputFile.Close()

	// Compress
	if m.Options.Compress {
		fileNameSegments := strings.Split(*m.OutputFilename, ".")

		if len (fileNameSegments) == 1 {
			fileNameSegments = append(fileNameSegments, "")
		}
		
		compOutFilename := fmt.Sprintf("%s.pack", strings.Join(fileNameSegments[0:len(fileNameSegments)-1], "."))
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

func (m Manager) getSchemas() ([]string, error) {
	rows, err := m.Database.Query("SELECT schema_name FROM information_schema.schemata")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var schemas []string
	for rows.Next() {
		var schemaName string
		err := rows.Scan(&schemaName)
		if err != nil {
			return nil, err
		}

		if schemaName == "information_schema" || schemaName == "pg_catalog" || schemaName == "pg_toast" {
			continue
		}
		schemas = append(schemas, schemaName)
	}

	return schemas, nil
}

func (m Manager) getTables(schema string) ([]string, error) {
	rows, err := m.Database.Query(`SELECT
		table_name
		FROM information_schema.tables
		WHERE table_schema=$1
		AND table_type='BASE TABLE'
	`, schema)
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

func (m Manager) getCreateTableStatement(tableName string, schema string) (string, error) {
	// Query retrieves column metadata for the given table from the PostgreSQL
	// information_schema and pg_catalog system tables. Joining the tables is to
	// fetch some additional info like the namespace for user-defined types.
	rows, err := m.Database.Query(`SELECT
			c.column_name,
			c.data_type,
			c.is_nullable,
			c.column_default,
			c.character_maximum_length,
			c.numeric_precision,
			c.numeric_scale,
			c.udt_name, --type name (user-defined/array types)
			n.nspname as type_schema --type schema
		FROM information_schema.columns c
		LEFT JOIN pg_catalog.pg_type t ON c.udt_name = t.typname --user-defined types
		LEFT JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
		WHERE c.table_name = $1 AND c.table_schema = $2
		ORDER BY c.ordinal_position;`,
		tableName, schema)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	columnDefs := make([]string, 0)
	for rows.Next() {
		var columnName, dataType, isNullable, columnDefault, udtName, typeSchema sql.NullString
		var characterMaximumLength, numericPrecision, numericScale sql.NullInt64
		err := rows.Scan(&columnName, &dataType, &isNullable, &columnDefault, &characterMaximumLength, &numericPrecision, &numericScale, &udtName, &typeSchema)
		if err != nil {
			return "", err
		}

		if !(columnName.Valid && dataType.Valid) {
			continue
		}

		columnDef := columnName.String

		if !udtName.Valid {
			return "", fmt.Errorf("invalid udtName for column %s", columnName.String)
		}

		if strings.HasPrefix(udtName.String, "_") {
			// Array type. example: _text
			columnDef += " " + udtName.String[1:] + "[]"
		} else if dataType.String == "USER-DEFINED" && typeSchema.Valid && typeSchema.String != "" {
			// User-defined type. example: public.text
			columnDef += " " + typeSchema.String + "." + udtName.String
		} else {
			// Standard pg_catalog type. example: int64
			columnDef += " " + dataType.String
		}

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
			defaultValue := columnDefault.String
			if strings.Contains(defaultValue, "nextval(") {
				seqPattern := `nextval\('([^']*)'::regclass\)`
				re := regexp.MustCompile(seqPattern)
				defaultValue = re.ReplaceAllStringFunc(defaultValue, func(match string) string {
					seqName := re.ReplaceAllString(match, "$1")
					if !strings.Contains(seqName, ".") {
						seqName = schema + "." + seqName
					}
					return fmt.Sprintf("nextval('%s'::regclass)", seqName)
				})
			}
			columnDef += " DEFAULT " + defaultValue
		}

		columnDefs = append(columnDefs, columnDef)
	}

	if len(columnDefs) == 0 {
		return "", fmt.Errorf("Table '%s' not found", tableName)
	}

	createTableStmt := fmt.Sprintf("CREATE TABLE %s.%s (\n\t%s\n);", schema, tableName, strings.Join(columnDefs, ",\n\t"))

	return createTableStmt, nil
}

func (m Manager) getPrimaryKeyStatements(tableName string, schema string) (string, error) {
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
			AND tc.table_schema=$1
			AND tc.table_name=$2;	
	`, schema, tableName)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var (
		pks            []string
		constraintName string
	)
	for rows.Next() {
		var pk string
		err := rows.Scan(&pk, &constraintName)
		if err != nil {
			return "", err
		}
		pks = append(pks, pk)
	}

	if len(pks) > 0 {
		statements = append(statements, fmt.Sprintf("ALTER TABLE ONLY %s.%s\n\tADD CONSTRAINT %s PRIMARY KEY (%s);", schema, tableName, constraintName, strings.Join(pks, ", ")))
	}

	return strings.Join(statements, "\n"), nil
}

func (m Manager) getForeignKeyStatements(tableName string, schema string) (string, error) {
	var statements []string

	rows, err := m.Database.Query(`
		SELECT
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
			AND tc.table_schema=$1
			AND tc.table_name=$2;
	`, schema, tableName)
	if err != nil {
		return "", err
	}

	for rows.Next() {
		var constraintName, _tableName, columnName, foreignTableSchema, foreignTableName, foreignColumnName string
		err := rows.Scan(&constraintName, &_tableName, &columnName, &foreignTableSchema, &foreignTableName, &foreignColumnName)
		if err != nil {
			return "", err
		}
		statements = append(statements, fmt.Sprintf("ALTER TABLE ONLY %s.%s\n\tADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s.%s(%s);",
			schema, _tableName, constraintName, columnName, foreignTableSchema, foreignTableName, foreignColumnName))
	}

	return strings.Join(statements, "\n"), nil
}

func (m Manager) getDomainStatements(schema string) (string, error) {
	result := []string{}

	query := `SELECT
				t.typname AS domain_name,
				pg_catalog.format_type(t.typbasetype, t.typtypmod) AS data_type,
				c.conname AS constraint_name,
				pg_get_constraintdef(c.oid, true) AS check_clause,
				d.domain_schema AS domain_schema,
				pg_catalog.pg_get_userbyid(t.typowner) AS owner
			FROM pg_catalog.pg_type t
			LEFT JOIN pg_catalog.pg_constraint c ON t.oid = c.contypid
			INNER JOIN information_schema.domains d ON t.typname = d.domain_name
			WHERE t.typtype = 'd' AND d.domain_schema = $1
			ORDER BY domain_name;`
	rows, err := m.Database.Query(query, schema)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			Name           string
			DataType       string
			Constraint     string
			Schema         string
			Owner          string
			CheckClauses   []string
			constraintName sql.NullString
			checkClause    sql.NullString
		)
		err := rows.Scan(&Name, &DataType, &constraintName, &checkClause, &Schema, &Owner)
		if err != nil {
			return "", err
		}

		if constraintName.Valid {
			Constraint = constraintName.String
		}
		if checkClause.Valid {
			CheckClauses = append(CheckClauses, checkClause.String)
		}

		var stmt string
		stmt += fmt.Sprintf("CREATE DOMAIN %s.%s AS %s", Schema, Name, DataType)
		for _, clause := range CheckClauses {
			stmt += fmt.Sprintf("\n    CONSTRAINT %s %s", Constraint, clause)
		}
		stmt += ";\n"
		stmt += fmt.Sprintf("\nALTER DOMAIN %s.%s OWNER TO %s;", Schema, Name, Owner)
		result = append(result, stmt)
	}

	if err = rows.Err(); err != nil {
		return "", err
	}

	return strings.Join(result, "\n\n"), nil
}

func (m Manager) getFunctionStatements(schema string) (string, error) {
	result := []string{}

	query := `SELECT
					n.nspname AS schema_name,
					p.proname AS function_name,
					p.provolatile AS volatile,
					p.proisstrict AS strict,
					pg_catalog.pg_get_function_arguments(p.oid) AS argument_types,
					pg_catalog.pg_get_function_result(p.oid) AS return_type,
					pg_catalog.pg_get_userbyid(p.proowner) AS owner,
					p.prosrc AS body,
					l.lanname AS language
			FROM pg_catalog.pg_proc p
			JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
			JOIN pg_catalog.pg_language l ON l.oid = p.prolang
			WHERE pg_catalog.pg_function_is_visible(p.oid)
			AND n.nspname = $1
			AND p.prokind = 'f' -- Only select normal functions
			ORDER BY schema_name, function_name;`
	rows, err := m.Database.Query(query, schema)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			Name          string
			Schema        string
			ArgumentTypes string
			ReturnType    string
			Owner         string
			Language      string
			Body          string
			Volatile      string
			IsStrict      bool
		)
		err := rows.Scan(&Schema, &Name, &Volatile, &IsStrict, &ArgumentTypes, &ReturnType, &Owner, &Body, &Language)
		if err != nil {
			return "", err
		}

		switch Volatile {
		case "i":
			Volatile = "IMMUTABLE"
		case "v":
			Volatile = "VOLATILE"
		case "s":
			Volatile = "STABLE"
		default:
			Volatile = ""
		}

		var stmt string
		Separator := "$_$"

		if Language == "plpgsql" {
			Separator = "$$"
		}
		stmt += fmt.Sprintf("CREATE FUNCTION %s.%s(%s) RETURNS %s", Schema, Name, ArgumentTypes, ReturnType)
		stmt += fmt.Sprintf("\n\tLANGUAGE %s %s %s", Language, Volatile, (map[bool]string{true: "STRICT", false: ""})[IsStrict])
		stmt += fmt.Sprintf("\nAS %s\n%s\n%s", Separator, Body, Separator)

		stmt += ";\n"
		stmt += fmt.Sprintf("\nALTER FUNCTION %s.%s(%s) OWNER TO %s;", Schema, Name, ArgumentTypes, Owner)
		result = append(result, stmt)
	}

	if err = rows.Err(); err != nil {
		return "", err
	}

	return strings.Join(result, "\n\n"), nil
}

func (m Manager) getSequenceStatements(schema string) (string, error) {
	var sequenceStatements []string

	query := `
	SELECT s.sequence_name,
		s.sequence_schema,
		s.start_value,
		s.minimum_value,
		s.maximum_value,
		s.increment,
		s.cycle_option,
		r.rolname
	FROM information_schema.sequences s
	JOIN pg_namespace n ON n.nspname = s.sequence_schema
	JOIN pg_class c ON c.relname = s.sequence_name AND c.relnamespace = n.oid
	JOIN pg_roles r ON r.oid = c.relowner
	WHERE s.sequence_schema =$1;
	`

	rows, err := m.Database.Query(query, schema)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			name         string
			startValue   int64
			minimumValue int64
			maximumValue int64
			increment    int64
			cycleOption  string
			schema       string
			owner        string
		)

		err := rows.Scan(&name, &schema, &startValue, &minimumValue, &maximumValue, &increment, &cycleOption, &owner)
		if err != nil {
			return "", err
		}

		cycle := ""
		if cycleOption == "YES" {
			cycle = "\n\tCYCLE"
		}

		minvalueClause := fmt.Sprintf("\n\tMINVALUE %d", minimumValue)
		if minimumValue == 1 {
			minvalueClause = "\n\tNO MINVALUE"
		}

		maxvalueClause := fmt.Sprintf("\n\tMAXVALUE %d", maximumValue)
		if maximumValue == PostgresBigintMax {
			maxvalueClause = "\n\tNO MAXVALUE"
		}

		createStmt := fmt.Sprintf("CREATE SEQUENCE %s.%s\n\tSTART WITH %d%s%s\n\tINCREMENT BY %d%s\n;\n",
			schema, name, startValue, minvalueClause, maxvalueClause, increment, cycle)

		alterStmt := fmt.Sprintf("ALTER SEQUENCE %s.%s OWNER TO %s;", schema, name, owner)

		sequenceStatements = append(sequenceStatements, createStmt+"\n"+alterStmt)
	}

	if err = rows.Err(); err != nil {
		return "", err
	}

	result := strings.Join(sequenceStatements, "\n\n")
	return result, nil
}

func (m Manager) getCreateTypeStatements(schema string) (string, error) {
	query := `
                SELECT
                        t.typname,
                        n.nspname,
                        pg_catalog.pg_get_userbyid(t.typowner) as owner,
                        t.typtype
                FROM
                        pg_catalog.pg_type t
                        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                WHERE
                        (n.nspname = $1 OR $1 = '')
                        AND t.typtype = 'e' -- Temporarily, select only enum types
						-- TODO: Add support for other types (range, composite, etc)
                        AND t.typisdefined = true;
        `
	rows, err := m.Database.Query(query, schema)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	typeDefinitions := make([]string, 0)
	for rows.Next() {
		var typeName, typeSchema, typeOwner, typtype string
		err := rows.Scan(&typeName, &typeSchema, &typeOwner, &typtype)
		if err != nil {
			return "", err
		}

		var createTypeStmt string
		switch typtype {
		case "e": // Enum type
			createTypeStmt, err = m.getCreateEnumTypeStatement(typeName, typeSchema)
			if err != nil {
				return "", err
			}

		default:
			continue
		}

		typeDefinitions = append(typeDefinitions, createTypeStmt)
	}

	if err := rows.Err(); err != nil {
		return "", err
	}
	return strings.Join(typeDefinitions, "\n"), nil
}

func (m Manager) getCreateEnumTypeStatement(typeName string, schema string) (string, error) {
	query := `
                SELECT
                        e.enumlabel
                FROM
                        pg_catalog.pg_enum e
                        JOIN pg_catalog.pg_type t ON e.enumtypid = t.oid
                        JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                WHERE
                        t.typname = $1 AND n.nspname = $2
                ORDER BY
                        e.enumsortorder;
        `
	rows, err := m.Database.Query(query, typeName, schema)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	labels := make([]string, 0)
	for rows.Next() {
		var label string
		if err := rows.Scan(&label); err != nil {
			return "", err
		}
		labels = append(labels, fmt.Sprintf("\t'%s'", label))
	}

	if err := rows.Err(); err != nil {
		return "", err
	}

	enumLabels := strings.Join(labels, ", \n")
	createTypeStmt := fmt.Sprintf("CREATE TYPE %s.%s AS ENUM (\n%s\n);", schema, typeName, enumLabels)
	return createTypeStmt, nil
}

func (m Manager) broadcastTableRecordsINSERT(tableName string, schema string, ch chan string) error {
	go func() {
		selectDataSQL := fmt.Sprintf("SELECT * FROM %s.%s", schema, tableName)
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

			insertStmt := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (",
				schema, tableName, strings.Join(columnNames, ", "))

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
							insertStmt += fmt.Sprintf("'%s'", t.Format("2006-01-02 15:04:05"))
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

func (m Manager) broadcastTableRecordsCOPY(tableName string, schema string, ch chan string) error {
	go func() {
		selectDataSQL := fmt.Sprintf("SELECT * FROM %s.%s", schema, tableName)
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

		ch <- fmt.Sprintf("COPY %s.%s (%s) FROM stdin;\n",
			schema, tableName, strings.Join(columnNames, ", "))

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
							valueParams = append(valueParams, fmt.Sprintf("'%s'", t.Format("2006-01-02 15:04:05")))
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

		ch <- "\\.\n"

	}()
	return nil
}
