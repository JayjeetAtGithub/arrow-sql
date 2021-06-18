build:
	g++ arrow_sql.cc -larrow -larrow_dataset -lparquet -lduckdb -o arrow_sql
