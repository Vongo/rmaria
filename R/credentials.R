# Resolve DB/HOST/PWD/USER from the calling context. Reproduces the historical
# parent-frame/parent-env scan used by the *q wrappers, but as one helper.
# Because it runs one frame deeper than the wrapper, dynamic-frame offsets are +1.
# NOTE: inherits=TRUE means creds set in globalenv (e.g. via load_env) are also found -- this matches the original wrappers' behavior and is intentional.
# Returns list(host, db, user, pwd) or stop()s if not all four are found.
resolve_credentials <- function() {
  needed <- c("DB", "HOST", "PWD", "USER")
  # safe() guards parent.env() on emptyenv/baseenv; parent.frame(n) beyond the stack does NOT error (returns globalenv).
  safe <- function(expr) tryCatch(expr, error = function(e) emptyenv())
  w <- parent.frame()  # the calling wrapper's environment
  envs <- list(
    w,
    safe(parent.frame(2)),
    safe(parent.env(w)),
    safe(parent.env(parent.env(w))),
    safe(parent.env(parent.frame(2))),
    safe(parent.env(parent.frame(3))),
    safe(parent.env(parent.frame(4))),
    safe(parent.env(parent.frame(5))),
    safe(parent.env(parent.frame(6)))
  )
  for (e in envs) {
    if (all(vapply(needed, exists, logical(1), envir = e, inherits = TRUE))) {
      return(list(
        host = get("HOST", envir = e), db = get("DB", envir = e),
        user = get("USER", envir = e), pwd = get("PWD", envir = e)
      ))
    }
  }
  stop("rmaria: credentials not found -- set DB, HOST, USER, PWD in the calling context. See ?load_env.",
       call. = FALSE)
}
