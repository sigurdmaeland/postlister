# Handover: postlister-prosjektet

Skrevet 2026-09-03, oppdatert 2026-09-13. 18 kommuner skrapes fordelt på fire plattformer: OpenGov (360online), ACOS ("nye-innsyn"), eInnsyn.no og Innsynsportal/360 ("Plan & Build"). Hver kommune har egen `common/scraper_lib.py` og samme struktur: `<periode>_dump/` for full historisk backfill, `running_daily/` for daglig endringslogg. Kun Gjøvik, Kristiansand, Lillestrøm og Sandnes er wheel-pakket for Databricks-deploy. Ingen silver-transform finnes ennå.

Dekningstallene under er talt med `python3 scripts/dump_stats.py --scan` - kjør den på nytt etter enhver ny full dump, tallene endrer seg fort (flere dumper har vokst/krympet mye mellom økter pga. ekstern re-skraping). **Viktig:** et undersett av dumpene under er kun smoketest-vinduer (kjørt med et tall-argument til `main.py`, f.eks. `python3 main.py 40`, ikke en reell full historisk kjøring) - se egen seksjon "Dumper som må kjøres fullt lokalt" helt nederst før du stoler på tallene for disse.

---

## OpenGov (360online)

Standardsøk på sakstype viser stille kun siste ~2 måneder; løsningen er å søke på sakstype-prefikset som fritekst (`q=BYGG-` osv.), som gir full historikk. Portalen har ikke datofilter eller "nytt siden sist"-API, så endringsloggen er løst med en daglig snapshot-diff av hele sakslisten.

**Kommune:** Kristiansand
**Innsynside:** OpenGov
**Dumper:** 2020-today_dump/[bygg, henv, tilsyn, ulov]
**Adressedekning:** 94,0%
**Gnr/bnr-dekning:** 98,6%
**Ingen eiendomsreferanse:** 1,0%

Kristiansand har en egen feltkode-sperre som filtrerer bort adressekandidater med et bokstav+1-2-tall-mønster rett foran husnummeret ("Eidet felt B3"), fordi dette nesten alltid er en sone- eller planreferanse og ikke en reell gateadresse - verifisert mot rundt 2000 saker med kun ett feiltreff. Ulovlighetssaker har et eget tittelmønster ("- GNR/BNR -") der gnr/bnr ofte mangler helt, og kildesystemet i stedet gjentar adressen med en etterhengt skråstrek som ren plassholdertekst - dette måtte gjenkjennes og luket bort separat. Enkelte saker gjelder flere eiendommer samtidig, med kommaseparerte bruksnummerlister ("426/55, 50, 47, 71, 54") og et helt eget matrikkelnummer plassert et annet sted i tittelen, som krevde egen håndtering for å ikke miste noen av eiendomsreferansene. Kristiansand var en av de første OpenGov-kommunene som ble satt opp, og har derfor også mest moden feilhåndtering rundt selve Azure-opplastingen - snapshotet for endringsloggen avanseres kun når dagens opplasting faktisk har lyktes, slik at ingen endringer går tapt ved en forbigående Azure-feil. Kommunen er ferdig skrapet med full historikk og er én av kun fire kommuner i hele prosjektet som er wheel-pakket for Databricks-deploy. `bygg`-dumpen ble kjørt på nytt i sin helhet 2026-09-12 (18 490 saker, ikke et rundt smoketest-tall); en gjennomgang av samtlige saker uten adresse (1 050) og uten gnr/bnr (177) - kjørt `address_from_tittel`/`extra_gnr_bnr_fra_tittel`/`ekstra_matrikkel_annet_sted_i_tittel` fra `common/scraper_lib.py` på nytt mot hver lagrede sakstittel - fant ingen tilfeller der dagens kode gjenkjenner mer enn det som allerede er lagret. Stikkprøve av titlene bekrefter at de gjenværende manglene er reelle (stedsnavn/infrastruktursaker uten husnummer eller matrikkelreferanse i tittelen), ikke uoppdagede parse-bugs.

**Kommune:** Lillestrøm
**Innsynside:** OpenGov
**Dumper:** 2020-today_dump/[bygg, henv, tilsyn, ulov]
**Adressedekning:** 92,6%
**Gnr/bnr-dekning:** 97,4%
**Ingen eiendomsreferanse:** 2,5%

Lillestrøm deler i praksis samme adresseparser som Kristiansand - scraperen ble bygget som en direkte videreføring/tilpasning av Kristiansand-koden, og arver dermed både feltkode-sperren og håndteringen av flere eiendommer i samme sak. Den største kommunespesifikke utfordringen har ikke vært selve tittelparsingen, men en strukturell endring: kommunenummeret ble endret fra 3030 til 3205 ved regionreformens omnummerering i 2024, og dette måtte håndteres som en eksplisitt konstant i koden for at matrikkelnummer skulle peke riktig videre. Byggesaksarkivet i seg selv er relativt konsistent, men henvendelses- og tilsynssakene har noe større spredning i tittelformat enn byggesakene, siden disse sakstypene sjeldnere følger en fast mal. Som hos Kristiansand er snapshotet for endringsloggen bygget med samme sikkerhetsmekanisme, der fremgangen kun lagres hvis selve opplastingen til Azure lyktes. Kommunen er ferdig skrapet med full historikk fra 2020 og er wheel-pakket for Databricks-deploy, sammen med Kristiansand og Sandnes.

**Kommune:** Sandnes
**Innsynside:** OpenGov
**Dumper:** 2022-today_dump/[bygg, henv, tilsyn, ulov]
**Adressedekning:** 92,2%
**Gnr/bnr-dekning:** 99,6%
**Ingen eiendomsreferanse:** 0,3%

Sandnes har en litt annen kildestruktur enn de andre OpenGov-kommunene: sakstittelen har en reell ANDRE LINJE i selve kildedataen, med en redundant oppsummering av adresse og matrikkelnummer sammen. Problemet er at rekkefølgen på denne andre linjen varierer fra sak til sak - noen ganger står adressen først, andre ganger matrikkelnummeret - så innholdet må klassifiseres komma-ledd for komma-ledd i stedet for å anta ett fast mønster. Tilsynssaker har i tillegg en egen utfordring: de skriver ofte gnr/bnr i ren tekst først i selve tittelen ("Gnr 65 bnr 81"), og denne varianten brukes som siste fallback når verken det strukturerte feltet fra API-et eller andrelinjen gir noe resultat. Kombinasjonen av disse tre uthentingsstrategiene gir samlet sett svært høy gnr/bnr-dekning, selv om ingen av dem alene dekker alt. Azure-opplastingen for Sandnes er nylig bygget og verifisert med mock-baserte tester, men er ikke bekreftet testet mot en ekte Azure-instans ennå - markert som uverifisert i prod. Kommunen er ferdig skrapet med full historikk og er wheel-pakket, som én av kun fire kommuner i prosjektet.

**Kommune:** Sarpsborg
**Innsynside:** OpenGov
**Dumper:** 2023-today_dump/[bygg]
**Adressedekning:** 93,7%
**Gnr/bnr-dekning:** 99,8%
**Ingen eiendomsreferanse:** 0,2%

Sarpsborg er den eneste OpenGov-kommunen der kun byggesak (BYGG-prefikset) er skrapet - henvendelser, tilsyn og ulovlighetssaker er bevisst utelatt fra omfanget, ikke en begrensning i selve teknikken. Tittelformatet er dessuten speilvendt sammenlignet med de tre andre OpenGov-kommunene: matrikkelnummeret ("GNR/BNR/FESTENR/SEKSJONSNR") står alltid FØRST i tittelen, etterfulgt av gatenavn og husnummer, og til slutt en beskrivelse ("2081/254/0/0 - Yvenveien 40 - tilbygg"). Dette krevde en egen parser fremfor å gjenbruke Kristiansand/Lillestrøm sin gate-først-logikk direkte. Et lite mindretall av sakene, typisk vann- og avløpssaker eller annen infrastruktur uten en enkel gateadresse, bryter dette faste mønsteret helt - disse håndteres med en løsere fallback som søker gjennom hele tittelen etter alle tall/tall-par den kan finne, uten å anta noen bestemt rekkefølge. Azure-opplasting er foreløpig ikke implementert i det hele tatt for Sarpsborg (kun en stub-funksjon som printer i stedet for å laste opp), så dataene ligger fortsatt bare lokalt. Kommunen er ferdig skrapet lokalt, men ikke wheel-pakket.

---

## ACOS ("nye-innsyn")

Felles API på tvers av kommunene; instanser med flere arkiv i samme installasjon krever en egen kildenøkkel i søket, ellers ignoreres filteret stille. Flere av kommunene bærer arven fra kommunesammenslåing: gårdsnummer fra innfusjonerte kommuner må offsettes for å peke riktig under dagens kommunenummer.

**Kommune:** Gjøvik
**Innsynside:** ACOS
**Dumper:** 2017-today_dump/[bygg]
**Adressedekning:** 93,3%
**Gnr/bnr-dekning:** 98,9%
**Ingen eiendomsreferanse:** 1,0%

Gjøvik er den enkleste ACOS-kommunen i prosjektet - alt data kommer fra én eneste arkivkilde, uten behov for noen egen kildenøkkel i søket slik Sandefjord, Tønsberg og Ålesund krever. Tittelen starter nesten alltid med selve matrikkelnummeret ("GNR/BNR[/FESTENR[/SEKSJONSNR]]"), men skilletegnet mellom dette og adressen varierer overraskende mye - noen ganger bindestrek, noen ganger komma, og noen ganger ingenting i det hele tatt, noe som krevde en fleksibel parser fremfor ett fast mønster. En kjent kilde-eiendommelighet er at "0/0" brukes som en eksplisitt placeholder for "intet gnr/bnr tilgjengelig", og denne må gjenkjennes og hoppes over i stedet for å bli tolket som en reell (og feil) eiendomsreferanse. Stedsnavn-haler som ofte følger etter selve husnummeret ("Rognstadvegen 14, 2827 Hunndalen") strippes bort før validering, siden de ellers kunne ødelegge sjekken på at adressen faktisk ender i et gyldig husnummer-mønster. Fildokument-endepunktet på Gjøvik-instansen bruker dessuten POST i stedet for GET, med en annen responsstruktur enn de andre ACOS-kommunene, noe som måtte håndteres separat i vedleggshentingen. Kommunen er ferdig skrapet med full historikk fra 2017 og er wheel-pakket, om enn uverifisert i produksjonsmiljø ennå.

**Kommune:** Ålesund
**Innsynside:** ACOS
**Dumper:** alesund-2010-2019_dump/[bygg], alesund-2020-2023_dump/[bygg], alesund-2024-today_dump/[bygg], orskog-2009-2019_dump/[bygg], sandoy-2015-2019_dump/[bygg], skodje-2015-2019_dump/[bygg]
**Adressedekning:** 56,5%
**Gnr/bnr-dekning:** 99,0%
**Ingen eiendomsreferanse:** 1,0%

Ålesund er den mest komplekse ACOS-kommunen strukturelt: seks separate arkiv ligger i samme installasjon og må hentes hver for seg med sin egen kildenøkkel - tre periodearkiv for dagens Ålesund, og tre historiske arkiv fra Ørskog-, Skodje- og Sandøy-kommunene som ble slått sammen med Ålesund 01.01.2020. Hvert av de tre historiske arkivene har sitt eget faste gnr-offset (Ørskog +600, Skodje +500, Sandøy +800) hentet fra Kartverkets offisielle oversikt over gnr-endringer ved sammenslåingen, slik at matrikkelnummeret peker riktig under dagens kommunenummer. Skodje-arkivet skiller seg fra de andre fem ved å ha et helt annet tittelformat som mangler gateadresse fullstendig - der står i stedet kun søker- eller eiernavn sammen med gnr/bnr, noe som gjør adresseparsing umulig for denne kilden uansett tilnærming. Instansen krever i tillegg en egen autentiseringsflyt utover et vanlig API-kall: et forutgående GET-kall må gjøres mot selve innsynssiden for å hente en anti-forgery-cookie, og riktig Referer/Origin-header må settes, ellers svarer serveren med en 302-omdirigering til Azure AD-innlogging i stedet for søkeresultater. Vedleggstilgangen varierer også markant mellom arkivene - Skodje/Ørskog har vesentlig lavere andel fritt nedlastbare vedlegg (rundt 30-36%) enn de øvrige (85-99%). Kommunen er kun skrapet med et smalt testvindu så langt, og selve adresseparsingen er ikke ferdigstilt/fikset ennå slik den er for de fleste andre ACOS-kommunene.


**Kommune:** Larvik
**Innsynside:** ACOS
**Dumper:** 2018-today_dump/[bygg, tilsyn]
**Adressedekning:** 89,6%
**Gnr/bnr-dekning:** 99,4%
**Ingen eiendomsreferanse:** 0,4%

Larvik ligger, i motsetning til de fleste andre ACOS-kommunene, på en delt flertenant-vert (innsynpluss.onacos.no) i stedet for en selvhostet instans på egen kommune-URL - kommunen velges i stedet via en PortalID i selve søket. Søkeendepunktet heter dessuten noe annet enn på de selvhostede instansene ("overviewInit" i stedet for "overview"), noe som først ble oppdaget ved å inspisere faktisk nettverkstrafikk i nettleseren - et rent POST-kall mot standardendepunktet gir bare en 302-omdirigering til innloggingssiden, selv med korrekt anti-forgery-cookie satt. Adresseformatet i Larvik-titlene er "Beskrivelse - Adresse - Gbnr GNR/BNR", altså med gnr/bnr sist i tittelen - motsatt rekkefølge av flere andre ACOS-kommuner som har matrikkelnummeret først. Det finnes to kilder for Larvik, bygg og tilsyn, men de aller fleste tilsyns- og ulovlighetssaker ligger i praksis som ordinære byggesaker med et eget tittelprefiks i stedet for i en egen tilsynskilde - tilsyn-arkivet har derfor kun 3 saker totalt i hele sin levetid (04.01.2018-18.12.2019), bekreftet i main.py sin egen docstring, så disse 3 er den KOMPLETTE tilsyn-historikken, ikke et testutvalg. Bygg-dumpen ble tidligere stående på nøyaktig 500 saker (et smoketest-tall), men ble kjørt full 2026-09-13 og står nå på 14 428 saker - en reell full historisk kjøring, ikke lenger et testutvalg. Underveis ble et ytelsesproblem i `run_full_dump`/`run_daily` i `common/scraper_lib.py` identifisert og fikset: `make_session()` (som gjør et ekte HTTP-kall for å sette anti-forgery-cookien) ble tidligere kalt sekvensielt i hovedtråden for HVER sak, FØR den ble sendt til trådpoolen - dette gjorde selve sesjonsoppsettet til en ren seriell flaskehals som aldri fikk nytte av `MAX_WORKERS=8`-parallelliteten. Fikset ved å flytte `make_session()`-kallet inn i selve arbeidertråden (ny wrapper `_fetch_case_new_session`); `save_every` ble samtidig senket fra 200 til 25 for hyppigere fremdriftsvisning/lagring underveis. Adresseparsingen holder god dekning på full skala (89,6% adresse, 99,4% gnr/bnr). 10 av 14 428 saker (0,07%) feilet under henting (nettverksfeil etter 3 forsøk) og mangler dermed data - disse plukkes automatisk opp ved neste kjøring av `python3 main.py` (gjenopptakbar - kun sakene som faktisk feilet forsøkes på nytt).

**Kommune:** Moss
**Innsynside:** ACOS
**Dumper:** 2019-12-19-today_dump/[bygg]
**Adressedekning:** 94,8%
**Gnr/bnr-dekning:** 99,1%
**Ingen eiendomsreferanse:** 0,8%

Moss skraper kun byggesak - dette ble bekreftet ved å lese gjennom alle tilgjengelige søkefiltre på selve instansen, det finnes rett og slett ingen egen tilsyns- eller ulovlighetskilde å hente fra der. Sakens eget "eiendom"-felt fra API-et er alltid tomt i praksis, uansett hvor komplett saken ellers er beskrevet, så adresse og gnr/bnr har alltid måttet utledes utelukkende fra selve sakstittelen - det finnes ingen strukturert fallback å lene seg på. Moss bruker "/sak/{id}" i stedet for "/details/{id}" for å hente detaljer, fordi sistnevnte endepunkt mangler et dokumenttype-felt som trengs for å skille journalposter fra hverandre riktig. Det konsekvente tittelformatet ("Adresse - GNR/BNR[, GNR/BNR] - beskrivelse") gjorde grunnparsingen relativt rett frem, men det tok likevel fem separate runder med fiksing å få dekningen opp på et solid nivå - blant annet måtte støtte legges til for "(tidl. X/Y)"-parenteser som viser tidligere matrikkelnummer, "m.fl."-haler som antyder flere involverte eiendommer, og flere ulike varianter av hvordan flere adresser i samme sak skrives ut. Kommunen er nå ferdig skrapet med god dekning på både adresse og gnr/bnr, men er ikke wheel-pakket.

**Kommune:** Sandefjord
**Innsynside:** ACOS
**Dumper:** 2017-today_dump/[bygg, tilsyn]
**Adressedekning:** 92,9%
**Gnr/bnr-dekning:** 99,3%
**Ingen eiendomsreferanse:** 0,7%

Adresseparsingen (`parse_adresse_ny`) har vært gjennom fem runder fiksing denne høsten, og de fire første rundenes strukturelle bugs (posisjonsavhengig gnr/bnr-søk som mistet adresse+gnr/bnr, hele setninger godtatt som falske adresser, stopword-kollisjoner mot ekte gatenavn, bokstavspenn på husnummer, kommunenummer-forveksling i skråstrek-uttrykk) er nå fulgt av en femte runde (2026-09-12) som gikk gjennom hele bygg_ny-korpuset (14 892 saker) og fant og fikset: bokstav-transponerte skrivefeil for "gbnr" ("gbrn", "gnbr", "gbn"), bar ledende "GNR/BNR" helt uten nøkkelord (både komma- og bindestrek-separert - sistnevnte dekket 46 av de opprinnelig ~170 sakene uten gnr/bnr), "m.fl."/"med flere" limt rett på adressen, og en rekke bygnings-/enhets-/saksreferanseord ("Bygg N", "Tomt N", "Felt N", "BFS N", "Seksjon N", "JNR ÅÅÅÅ.NNN", "Bolig N", "Leilighet N", "byggetrinn N", "seks.nr N-N") som tidligere ble feilaktig godtatt som en egen, andre adresse. Alle fiksene er verifisert mot en full-korpus-regresjon (sammenligning av gammel lagret verdi mot ny parsing for samtlige 15 269 saker i bygg_ny og tilsyn_ny) før patching, og selve JSON-dumpene (`sandefjord_bygg.json`, `sandefjord_tilsyn.json`) er nå oppdatert med de nye verdiene - tallene over reflekterer altså den faktiske, ferdig-patchede dekningen, ikke bare hva koden er i stand til. Gjenværende saker uten adresse ble stikkprøvekontrollert og består i all hovedsak av saker der selve sakstittelen genuint mangler et husnummer (kun gatenavn/områdenavn oppgitt), ikke uoppdagede parse-bugs.

**Kommune:** Tønsberg
**Innsynside:** ACOS
**Dumper:** 2020-today_dump/[bygg, tilsyn], re-2006-2019_dump/[bygg], tonsberg-2006-2019_dump/[bygg]
**Adressedekning:** 90,4%
**Gnr/bnr-dekning:** 97,8%
**Ingen eiendomsreferanse:** 1,2%

Tønsberg har fire datakilder: dagens Tønsberg (2020-i dag, med både byggesak og tilsyn), det gamle Tønsberg-arkivet (2006-2019, før sammenslåingen), og et helt eget, separat arkiv fra tidligere Re kommune (også 2006-2019). Dette er det mest inkonsekvente tittelformatet i hele ACOS-settet - det gamle Re-arkivet blander hele tre ulike tittelformat om hverandre i samme fil, med og uten et valgfritt "RE -"-prefiks, kommaseparert format, og gnr/bnr både først og sist i tittelen avhengig av hvilken saksbehandler som skrev den opprinnelig. Et gnr-offset på +300 er påkrevd for alle ekte Re-opprinnelige saker for at matrikkelnummeret skal peke riktig under dagens kommunenummer, bekreftet mot Kartverkets offisielle oversikt over gnr-endringer ved sammenslåingen 01.01.2020 (Tønsberg selv uendret, +0). Under en grundig gjennomgang ble det oppdaget at et antall Re-opprinnelige saker var feilarkivert inn i det gamle Tønsberg-arkivet i stedet for Re-arkivet - kjennetegnet på et gjenværende "RE -"-prefiks i tittelen selv om saken lå i feil kilde - og disse manglet dermed det påkrevde +300-offsettet helt, funnet og rettet ved å kryssjekke mot dagens system der samme eiendom dukket opp igjen med et gnr presist 300 høyere. Begge de historiske arkivene har, i motsetning til hva man skulle forvente, bekreftet offentlig nedlastbare vedlegg. Kommunen er nå ferdig skrapet med god dekning på tvers av alle fire kilder.

---

## eInnsyn.no

Nasjonal fellesløsning i stedet for egen postlisteportal. Byggesaker skilles ofte ut fra resten av arkivet med en ekskluderingsliste på tittel-nøkkelord, siden arkivenhetene også inneholder reguleringssaker, tilsyn m.m.

**Kommune:** Bodø
**Innsynside:** eInnsyn
**Dumper:** 2024-today_dump/[bygg]
**Adressedekning:** 72,1%
**Gnr/bnr-dekning:** 4,6% (forventet lav - se under)
**Ingen eiendomsreferanse:** 24,1%

Bodø skiller seg fra Fredrikstad og Oslo ved at arkivenheten som mottar byggesakene ("Byggesaksavdelingen") er smal nok til at et rent type- og arkivskaper-filter holder alene - det er ikke nødvendig med noen ekskluderingsliste på tittelnivå slik de to andre eInnsyn-kommunene krever, bekreftet ved stikkprøve av 300 av over 9000 treff. Utfordringen med Bodø ligger i stedet i selve tittel-teksten: det finnes ingen kuratert liste over gatenavn-endelser å validere mot, siden egennavnvariasjonen er for stor ("Sprinten", "Kvilut", "Bukken Bruse" og lignende) - validering må derfor være rent strukturell, basert på stor forbokstav og et gyldig husnummer-mønster på slutten, uten noen semantisk sjekk av selve gatenavnet. Titlene oppgir dessuten nesten aldri et faktisk matrikkelnummer, kun gateadresse - den lave gnr/bnr-dekningen er derfor en dokumentert, forventet egenskap ved kildedataen og ikke et tegn på mangelfull parsing. En kildesystem-egenhet verdt å merke seg: publiseringsdatoen for praktisk talt alle Bodø-saker klumper seg rundt migreringsdatoen 17.06.2024, ikke sakens faktiske opprettelsesdato - saksnumre i selve tittelen går derimot tilbake til 2001, så den reelle historikken er lengre enn publiseringsdatoene alene skulle tilsi. Bodø har for øvrig reell vedleggstilgang via eInnsyn, bekreftet med direkte nedlasting av ekte PDF-dokumenter. Kommunen er ferdig skrapet med full dump.

**Kommune:** Fredrikstad
**Innsynside:** eInnsyn
**Dumper:** publisert-2024-today_dump/[bygg]
**Adressedekning:** 82,3%
**Gnr/bnr-dekning:** 87,2%
**Ingen eiendomsreferanse:** 8,4%

Fredrikstad bruker samme nasjonale eInnsyn-løsning som Bodø og Oslo, men arkivenhetene som mottar byggesaker her er sammensatt av to virksomheter ("Regulering og byggesak" og "Byggesak og geomatikk") som begge inneholder betydelig mer enn rene byggesaker - derfor kreves en ekskluderingsliste på tittelnivå for å luke ut irrelevante treff, på samme måte som for Oslo. Den store fordelen med Fredrikstad sammenlignet med de to andre eInnsyn-kommunene er at gnr/bnr nesten alltid er eksplisitt merket i selve tittelen med ordet "Eiendom" eller formen "Gnr X, bnr Y" - dette gjør uthentingen vesentlig mer presis enn Oslos mer frittstående søk etter tallpar i løpende tekst, siden selve etiketten forteller parseren nøyaktig hvor i tittelen den skal lete. Adressen utledes ved å først maskere bort denne gnr/bnr-blokken fra tittelen, og deretter lete etter et gate- og husnummer-mønster i det som blir igjen. Som Bodø har Fredrikstad reell vedleggstilgang via eInnsyn sitt API, i motsetning til Oslo. Historikken i den skrapede dumpen dekker en relativt kort periode (fra 2024), siden dette er datoen for da byggesaksdataene ble reelt publisert i eInnsyn for kommunen. Kommunen er ferdig skrapet med full dump for denne perioden.

**Kommune:** Oslo
**Innsynside:** eInnsyn
**Dumper:** 2018-today_dump/[bygg]
**Adressedekning:** 90,9%
**Gnr/bnr-dekning:** 1,9% (forventet lav - se under)
**Ingen eiendomsreferanse:** 8,8%

Oslo er den klart mest kompliserte eInnsyn-kommunen. For det første deler ikke Oslo vedleggene til sakenes postjournaler i det hele tatt gjennom eInnsyn - verken sakstittelen eller journalpostdataene inneholder noe vedleggsfelt for Oslo-saker, i motsetning til Bodø og Fredrikstad. Vi kompenserer for dette ved å bygge en direktelenke for hver sak til Oslo kommunes egen PBE-saksinnsynside (Plan- og bygningsetaten sitt eget system), utledet direkte fra eInnsyns saksnummer uten noe ekstra API-kall - denne lenken legges ved i tillegg til selve eInnsyn-dataene, slik at man kan klikke seg videre for å se faktiske vedlegg der. For det andre inneholder arkivenheten "Plan- og bygningsetaten" i eInnsyn mye mer enn byggesaker - reguleringsplaner, klagesaker, oppmålingsforretninger og lignende - og det finnes ingen egen sakstype-kode som skiller disse fra hverandre. Vi har derfor filtrert oss fram til de reelle byggesakene ved å ekskludere rundt 23 kjente tittelmønstre (blant annet "opprettelse av grunneiendom", "arealoverføring", "grensejustering", "regulering", "tilsyn", "oppmålingsforretning" og "matrikkelen"), en liste som er hentet direkte fra Oslo kommunes eget lagrede søkefilter for "byggesaker" på einnsyn.no, ikke egendefinert av oss. Som hos Bodø oppgir titlene nesten aldri et reelt matrikkelnummer, kun gateadresse, så den lave gnr/bnr-dekningen er forventet også her. Ingen saker finnes i arkivet før 2018. Kommunen er ferdig skrapet med full dump innenfor dette omfanget.

---

## Innsynsportal / 360 ("Plan & Build")

Felles GraphQL-plattform. To ulike pagineringsbegrensninger driver ulike løsninger: Asker/Drammen har en kjent bug der datofilteret på saker returnerer feil data (løst ved å hente journalposter dato-basert i stedet), mens Trondheim/Tromsø mangler offset-paginering helt og henter dag for dag.

**Kommune:** Asker
**Innsynside:** Innsynsportal
**Dumper:** 2020-today_dump/[bygg, henv, ulov]
**Adressedekning:** 39,5%
**Gnr/bnr-dekning:** 91,8%
**Ingen eiendomsreferanse:** 8,0%

Asker har et adresseformat der gnr/bnr kommer først i tittelen, etterfulgt av selve adressen og til slutt en beskrivelse, med både vanlig bindestrek og tankestrek brukt som skilletegn om hverandre. Den mest særegne utfordringen for Asker er at "henv" (henvendelser) ikke er en egen sakstype i selve portalen i det hele tatt - det finnes ingen sakstype som heter noe sånt blant de reelle typene. I stedet er "henv" en avledet, filtrert visning av byggesak-dataene: enhver byggesak uten noen eiendomsreferanse overhodet - verken fra det strukturerte API-feltet eller fra selve tittelen - klassifiseres om til "Henvendelse". Den lave gnr/bnr-dekningen totalt sett er derfor delvis strukturelt garantert av selve definisjonen, ikke bare en svakhet ved parsingen - disse sakene finnes for øvrig fortsatt i den ordinære byggesak-outputen også, ikke eksklusivt flyttet over. En kjent bug i selve kildesystemet (Elements) fører til at flere separate eiendomsnummer-par noen ganger slås sammen til ett langt tall uten noe skilletegn mellom dem - slike tilfeller forkastes bevisst i stedet for å gjette, og parseren faller da tilbake på å tolke selve tittelteksten. Asker var også kommunen der prosjektets kjente paginerings-bug i selve API-et først ble oppdaget - datofilteret på sakssøket teller riktig antall treff, men returnerer noder fra helt andre tidsperioder, løst ved å hente journalposter dato-basert og slå opp hver sak via saksnummer i stedet. Kommunen er ferdig skrapet med full historikk fra 2020.

**Kommune:** Drammen
**Innsynside:** Innsynsportal
**Dumper:** 1800-today_dump/[bygg], 2020-today_dump/[bygg]
**Adressedekning:** 42,6%
**Gnr/bnr-dekning:** 99,5%
**Ingen eiendomsreferanse:** 0,2%

Drammen har to helt atskilte arkiv med hvert sitt tittelformat: dagens aktive liste (2020-i dag), der adressen alltid står først i tittelen, og et historisk arkiv som - bekreftet ved å faktisk se på datofordelingen i selve dataene, ikke bare stole på mappenavnet - reelt går helt tilbake til 1800-tallet, med tyngdepunktet av sakene likevel liggende i perioden 2000-2019. Det historiske arkivet har et helt annet tittelformat ("Gbnr. gnr/bnr. Gate nr, Sted" etterfulgt av sakstype på egen linje), håndtert av en helt separat parsingsfunksjon fra dagens arkiv. Arkivet slår i tillegg sammen data fra tre tidligere kommuner - Drammen, Nedre Eiker og Svelvik, som ble slått sammen i 2020 - og saksnummer er kun garantert unikt INNENFOR hvert enkelt sub-arkiv, ikke på tvers, så hver sak må nøkles på kombinasjonen av sub-arkiv og saksnummer for å unngå kollisjoner. En kjent kildesystem-egenhet i det historiske arkivet: 1. januar hvert år har et unormalt høyt antall journalposter registrert, tydeligvis en plassholderdato brukt under skanning av gamle papirsaker der den opprinnelige datoen manglet. Drammen har til tross for disse kompleksitetene den høyeste datakvaliteten av samtlige kommuner i hele prosjektet, med svært høy gnr/bnr-dekning og nesten ingen saker uten eiendomsreferanse. Kommunen er ferdig skrapet med full historikk på begge arkiv.

**Kommune:** Tromsø
**Innsynside:** Innsynsportal
**Dumper:** 2019-10-22-today_dump/[bygg, henv, tilsyn, ulov]
**Adressedekning:** 86,0%
**Gnr/bnr-dekning:** 95,6%
**Ingen eiendomsreferanse:** 3,8%

Tromsø har et adresseformat som er speilvendt av både Trondheim og Asker: matrikkelnummeret står FØRST i tittelen ("GNR/BNR[/FESTE[/SEKSJON]] Gatenavn Nummer, beskrivelse"), skilt fra selve adressen med et rent mellomrom i stedet for bindestrek eller komma som ellers er vanlig. Det strukturerte eiendomsfeltet fra selve API-et er ofte tomt selv når tittelen tydelig inneholder gnr/bnr, så tittel-parsing brukes nesten alltid i tillegg til det strukturerte feltet, ikke bare som fallback. En reell plattformmigrasjon ble bekreftet ved binærsøk gjennom datoene: dagens fire samlede sakstyper gir konsekvent null treff før nøyaktig 22.10.2019 - før den datoen brukte Tromsø en helt annen, mer finkornet type-taksonomi ("BYG-STD", "BYG-ULOVL" og lignende koder), så denne datoen er en reell systemgrense og ikke en vilkårlig avgrensning satt av oss. En parsingsbug som nylig ble oppdaget og rettet: skråstrek-husnumre på samme gate, som "Bankgata 9/11", ble aldri fanget opp av det opprinnelige tallmønsteret (som kun støttet bindestrek-spenn som "9-11") og resulterte i at adressen ble stående som null - dette splittes nå riktig til to separate adresser. Kommunen er ferdig skrapet med full historikk fra plattformmigrasjonen i 2019 og har god dekning på tvers av alle fire sakstyper.

**Kommune:** Trondheim
**Innsynside:** Innsynsportal
**Dumper:** 2017-03-24-today_dump/[bygg, henv, tilsyn, ulov]
**Adressedekning:** 91,5%
**Gnr/bnr-dekning:** 97,5%
**Ingen eiendomsreferanse:** 1,6%

Trondheim har fire sakstyper (Byggesak, Henvendelse, Tilsynssak, Ulovlighetssak), hver med sin egen unike type-identifikator i selve API-spørringen, så det er ikke nødvendig med noen heuristikk for å skille dem fra hverandre slik enkelte andre kommuner må gjøre. Alle fire sakstyper deler likevel nøyaktig samme adresseparser, siden tittelformatet er identisk på tvers av dem: "Adresse [- Beskrivelse]", eventuelt innledet av "Eiendommen gnr/bnr" - som også finnes stavet "Eieindom" i kildedataen, en kjent skrivefeil som måtte håndteres eksplisitt. I eldre saker skiller ikke selve saksnummer-sekvensen pålitelig mellom sakstypene, siden alle delte samme "BYGG-"-prefiks før de nyere typene fikk egne prefiks som "HENV-" og "ULOV-" - derfor er selve type-ID-en fra API-et den eneste pålitelige måten å skille sakstype på. To parsingsbugger ble nylig oppdaget og rettet: skråstrek-husnumre på samme gate ("Harald Hårfagres gate 6/8") ble ikke fanget opp, på samme måte som i Tromsø, og "byggetrinn N" (en referanse til byggefase, som i "Grilstad Marina B3 byggetrinn 3") ble tidligere feilaktig lest som en del av selve gateadressen i stedet for å bli avvist som beskrivelsestekst. Den daglige endringsloggen for Trondheim skriver bevisst ingen lokal fil i repoet, og går rett til Azure Blob, siden jobben kjører kontinuerlig på tvers av fire sakstyper og en varig lokal kopi ville vokst ubegrenset. Kommunen er ferdig skrapet med full historikk fra 2017 og har god dekning på tvers av alle fire sakstyper.

---

## Dumper som må kjøres fullt lokalt

Alle scraperne kjøres via `python3 main.py` (uten argumenter) i den aktuelle `<periode>_dump/<kilde>/`-mappen for en reell full historisk kjøring; med ett tallargument (f.eks. `python3 main.py 40`) kjøres i stedet et smoketest på kun de N nyeste sakene. Gjennomgangen denne runden (2026-09-12) fant følgende dumper der antallet lagrede saker er mistenkelig lavt/rundt sammenlignet med forventet arkivstørrelse, og som derfor mest sannsynlig er igjenglemte smoketest-kjøringer og IKKE reell full historikk - disse må kjøres på nytt lokalt (uten Azure siden auto-opplasting er fjernet fra `run_full_dump` for disse, jf. tidligere oppgave) før statistikkene over kan stoles på:

**Ålesund** (alle seks arkiv, `bronze/postlister/acos/alesund/`) - samtlige har påfallende lave/runde tall: `alesund-2010-2019_dump/bygg` (85), `alesund-2020-2023_dump/bygg` (285), `alesund-2024-today_dump/bygg` (5), `orskog-2009-2019_dump/bygg` (50), `sandoy-2015-2019_dump/bygg` (50), `skodje-2015-2019_dump/bygg` (3 - `scraper_lib.py` sin egen docstring nevner selv at parsingen ble "bekreftet mot 300 ekte titler", altså finnes det langt flere reelle saker enn de 3 som ligger i dumpen nå). Kjør `python3 main.py` i hver av de seks `bygg/`-mappene.

Alle andre dumper i repoet (Kristiansand, Lillestrøm, Sandnes, Sarpsborg, Gjøvik, Moss, Sandefjord, Tønsberg, Bodø, Fredrikstad, Oslo, Asker, Drammen, Tromsø, Trondheim, **Larvik** - kjørt full 2026-09-13, se Larvik-seksjonen over) har sakstall som stemmer med reelle fulle historiske kjøringer og trenger ikke kjøres på nytt av denne grunnen alene.

---

## Kjøre stats på nytt

```bash
python3 scripts/dump_stats.py --scan
```

Skanner alle "master"-dump-JSON-filer under `bronze/postlister/` og skriver oppdaterte tall til `scripts/dump_stats.json`.
