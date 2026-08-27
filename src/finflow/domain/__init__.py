"""Pure logic: calendars, costs, metrics, the expression AST and the evaluator.

No IO, no clock, no globals. Every function here is callable from a REPL with
nothing running. Enforced by ``.importlinter`` and by the guard in
``tests/test_no_ambient_time.py``.
"""
