# Sandnes - kopi av common/scraper_lib.py, pakket inn i wheelen for
# Databricks. Se BRONZE_README.md for hvorfor det er en fysisk kopi og
# ikke en delt import.
#
# Dette er nå den ENESTE kopien i running_daily/ (tidligere lå det én
# identisk kopi per sakstype-mappe - tre kopier av samme fil, ren
# duplisering siden sakstypene deler 100% av skrapelogikken). Se
# app/main.py for hvorfor alle tre sakstyper nå bygges som én wheel.
