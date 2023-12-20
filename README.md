# PG PACK

**(This is a volunteer project associated with the Database Systems course taught by Dr. Mehrdad Ahmadzadeh Raji at Shahid Beheshti University)**

Pack your PostgreSQL databases quicker and more efficient with `pg_pack`. Compared to `pg_dump`, It performs up to 3x faster and uses [brotli compression](https://github.com/google/brotli) to provide up to 20x lighter data packages.

## Precautions

- As of now, `pg_pack` only supports ENUM types. Support for other types are to be implemented.
- Restoring files compressed by `pg_pack` to the database is only possible via `pg_pack` itself and not other tools like `pg_restore` or `psql`.

## Comparison

Comparison charts TBD.

## TODO

- [x] Implement brotli compression
- [x] Setup GitHub Actions to release binaries
- [x] Fix compressed output filename template
- [x] Pack SEQUENCES
- [x] Pack FUNCTIONS
- [x] Pack DOMAINS
- [x] Pack TYPES
- [ ] Pack AGGREGATE FUNCTIONS
- [ ] Pack VIEWS
- [ ] Data-only mode
- [ ] Schema-only mode
- [ ] Implement restore compressed
- [ ] Comparison charts (vs pg_dump) for README

## License

Distributed under the MIT License. See [`LICENSE`](https://github.com/SoroushTaheri/pg_pack/blob/main/LICENSE) for more information.

## Contact

Soroush Taheri

- Email: soroushtgh@gmail.com
