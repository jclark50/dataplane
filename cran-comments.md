## Test environments

* Local Windows 11, R 4.5.0
* Additional R-devel and cross-platform checks are pending before submission.

## R CMD check results

The local CRAN-style check completed with 0 errors, 0 warnings, and 1 note.
The sole note identifies this as a new submission.

The package URL audit found no unavailable URLs. Tests also pass when no
Python executable or managed PyArrow environment is available; the optional
delta-backend tests skip with an explanatory message.

## Optional Python integration

The package works without Python. The exported `dp_delta_setup()` function is
an explicit, user-initiated action that creates an isolated environment under
`tools::R_user_dir()`, installs a fixed PyArrow version, validates it, and
records its path. No downloads occur during package installation, loading,
examples, vignettes, or CRAN tests. `dp_delta_remove()` removes the managed
environment and saved configuration.

## Reverse dependencies

This is a new submission and has no reverse dependencies.
