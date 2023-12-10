# PG PACK

**(This is a volunteer project associated with the Database Systems course taught by Dr. Mehrdad Ahmadzadeh Raji at Shahid Beheshti University)**

Pack your PostgreSQL databases quicker and more efficient with `pg_pack`. Compared to `pg_dump`, It performs up to 3x faster and uses [brotli compression](https://github.com/google/brotli) to provide up to 20x lighter data packages.

_Note that restoring files compressed by `pg_pack` to the database is only possible via `pg_pack` itself and not other tools like `pg_restore` or `psql`._

# Comparison

Comparison charts TBD.

# TODO

- [x] Implement brotli compression
- [x] Setup GitHub Actions to release binaries
- [ ] Fix compressed output filename template
- [ ] Comparison charts (vs pg_dump) for README
- [ ] Implement restore compressed

# License

Distributed under the MIT License. See [`LICENSE`](https://github.com/SoroushTaheri/pg_pack/blob/main/LICENSE) for more information.

# Contact

Soroush Taheri

- Email: soroushtgh@gmail.com
