# Lillestrøm - kopi av common/scraper_lib.py, pakket inn i wheelen for
# Databricks. Se README.md i repo-roten ("running_daily sitt state") for
# hvorfor det er en fysisk kopi og ikke en delt import.
#
# Dette er nå den ENESTE kopien i running_daily/ (tidligere lå det én
# identisk kopi per sakstype-wheel - fire kopier av samme 1230-linjers fil,
# ren duplisering siden sakstypene deler 100% av skrapelogikken). Se
# app/main.py for hvorfor alle fire sakstyper nå bygges som én wheel.
