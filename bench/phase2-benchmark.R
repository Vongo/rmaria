# Usage: RMARIA_TEST_HOST=127.0.0.1 RMARIA_TEST_PORT=33306 RMARIA_TEST_USER=root \
#        RMARIA_TEST_PWD=test RMARIA_TEST_DB=rmaria_test Rscript bench/phase2-benchmark.R
pkgload::load_all(".")
e <- list(host=Sys.getenv("RMARIA_TEST_HOST","127.0.0.1"), port=as.integer(Sys.getenv("RMARIA_TEST_PORT","3306")),
          db=Sys.getenv("RMARIA_TEST_DB"), user=Sys.getenv("RMARIA_TEST_USER"), pwd=Sys.getenv("RMARIA_TEST_PWD"))
N <- 50000L
df <- data.frame(id=seq_len(N), a=seq_len(N)*2L, b=seq_len(N)*3L)
con <- .maria_connect(e$host, e$port, e$db, e$user, e$pwd); on.exit(RMariaDB::dbDisconnect(con))
RMariaDB::dbExecute(con, "DROP TABLE IF EXISTS bench_t")
RMariaDB::dbExecute(con, "CREATE TABLE bench_t (id INT PRIMARY KEY, a INT, b INT)")
cat(sprintf("insert %d: %.2fs\n", N, system.time(insert_table(df, "bench_t", host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd, progress_bar=FALSE))[["elapsed"]]))
df$a <- df$a + 1L
cat(sprintf("upsert %d: %.2fs\n", N, system.time(upsert_table(df, "bench_t", keycols="id", host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd, progress_bar=FALSE))[["elapsed"]]))
df$b <- df$b + 1L
cat(sprintf("update %d: %.2fs\n", N, system.time(update_table(df, "bench_t", keycols="id", host=e$host, port=e$port, db=e$db, user=e$user, password=e$pwd, progress_bar=FALSE))[["elapsed"]]))
RMariaDB::dbExecute(con, "DROP TABLE IF EXISTS bench_t")
