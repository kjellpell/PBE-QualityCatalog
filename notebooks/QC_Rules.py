# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# =============================================================================
# QC_Rules
#
# The rule catalogs, one cell per rule group. Each cell holds a YAML document
# — the authoring reference is in `RULES_GUIDE.md`.
#
# This notebook only defines values; run it via `%run QC_Rules`.
# =============================================================================

# Catalog name -> YAML document. The engine loads catalogs in
# alphabetical order of the key, so the key sets the run order.
RULE_CATALOG_SOURCES = {}


RULE_CATALOG_SOURCES["faktura"] = r"""
rule_group: Faktura
table: fakturalinjer
database: saksbehandling
description: Datakvalitet på fakturalinjer som kommer fra PB360
pk_column: fakturanr

rules:

# Én regel per kolonne, slik at rule_id peker på nøyaktig én feil.
- rule_id: FAK-001
  name: fakturanr mangler
  description: 'Alle fakturalinjer må ha et fakturanummer.'
  check: fakturanr IS NOT NULL

- rule_id: FAK-002
  name: linje_belop mangler
  description: 'Alle fakturalinjer må ha et beløp.'
  check: linje_belop IS NOT NULL

# De neste gjør tekniske sjekker på fakturalinjene
- rule_id: FAK-003
  name: Alle fakturalinjer må ha samme sum som total
  description: 'Summen av alle linje_belop-verdier for en gitt Fakturanr må være lik
    Faktura_belop registrert på fakturahodet (innenfor en toleranse på 1 krone).'
  aggregate_matches:
    group_column: fakturanr
    aggregate_column: linje_belop
    reference_column: fakturasum
    aggregate: sum
    tolerance: 1
"""


RULE_CATALOG_SOURCES["faser"] = r"""
rule_group: Faser
table: faser
database: saksbehandling
description: Validering av datakvalitet for faser
pk_column: pk_faser

# Jeg har valgt å kun se på PB360-saker per nå
where: fagsystem = 'PB360'

# joins:
#- table: saksbehandling.saker
#  left_on: to_case_recno
#  right_on: case_recno
#  how: left
#  select:
#  - case_recno
#  - saksnummer
#  - saksansvarlig_kode

rules:

# Én regel per kolonne, slik at rule_id peker på nøyaktig én feil.
- rule_id: FAS-001
  name: pk_faser mangler
  check: pk_faser IS NOT NULL

- rule_id: FAS-002
  name: fk_saker mangler
  check: fk_saker IS NOT NULL

# Sjekk om stage_recno er unik
- rule_id: FAS-003
  name: pk_fkaser må være unik på tvers av alle faser
  unique:
  - pk_faser

# when: avgrenser hvilke rader regelen gjelder for. Her: kun lukkede faser.
- rule_id: FAS-004
  name: Lukket fase mangler fase_lukket_dato
  description: 'Når siste milepæl er satt, må fasen også være lukket.'
  when: seneste_stoppmilepael_dato IS NOT NULL
  check: fase_lukket_dato IS NOT NULL

- rule_id: FAS-005
  name: Lukket fase mangler tidligste_startmilepael_dato
  description: 'Når siste milepæl er satt, må startdato være kjent.'
  when: seneste_stoppmilepael_dato IS NOT NULL
  check: tidligste_startmilepael_dato IS NOT NULL

# Formålet med denne regelen er å unngå at det finnes åpne faser i en sak,
# uten at saken har en saksbehandler.
#- rule_id: FAS-006
#  name: Åpen fase mangler saksbehandler
#  description: 'Alle faser hvor sluttdato er null må være tildelt en saksbehandler.'
#  when: seneste_stoppmilepael_dato IS NULL
#  check: saksansvarlig_kode IS NOT NULL

# Dette er en viktig regel, for å sikre at tidsbruk blir beregnet riktig
- rule_id: FAS-007
  name: Sluttdato kan ikke være før startdato
  description: 'For ferdige faser må dato for vedtak/beslutning ikke være tidligere enn mottatt dato.'
  check: seneste_stoppmilepael_dato >= tidligste_startmilepael_dato

# Sjekk på tidsbruk er egentlig bare en sikkerhetsventil, siden vi får dette tallet fra PB360 direkte
- rule_id: FAS-008
  name: Tidsbruk kan ikke være negativt tall
  description: 'Negativ tidsbruk er et tegn på feil registrering av start- eller sluttdato.'
  check: tidsbruk >= 0

# Bransjetid beregner vi imidlertid selv, og må derfor ha en sjekk.
- rule_id: FAS-009
  name: Bransjetid kan ikke være negativt tall
  description: 'Negativ bransjetid er et tegn på feil registrering av start- eller sluttdato.'
  check: bransjetid >= 0

# Denne sjekker om fristforlengelsen er lengre enn tillatt
- rule_id: FAS-010
  name: Forlenget frist kan ikke være lengre enn 126 dager for saker til politisk behandling
  description: 'Forlenget frist kan ikke være lengre enn 126 dager for saker til politisk behandling.'
  when: indikator IN ('Til politisk behandling')
  check: frist_dager <= 126


  # Fordi det ikke er noen begrensning på fristforlengelse i PB360, må vi sjekke de
# indikatorene som normalt _ikke_ skal ha fristforlengelse. Se også regelen under.
- rule_id: FAS-012
  name: Opprinnelig frist og frist_dager må være lik for indikatorer uten fristforlengelse
  description: 'For faser uten fristforlengelse må opprinnelig_frist være lik frist_dager.'
  when: >
    indikator IN ('Delesak 3 uker', 'Delesak 12 uker', 'Byggesak 3 uker',
                  'Byggesak 12 uker', 'Endringstillatelse', 'Igangsettingstillatelse',
                  'Brukstillatelse', 'Ferdigattest', 'Seksjonering', 'Reseksjonering',
                  'Oppstartsmøte')
  check: opprinnelig_frist_dager = frist_dager
  
# Her er det en del avvik, og det stusser jeg litt på
- rule_id: FAS-011
  name: decisiondate kan ikke være lavere enn sluttdato
  description: 'For ferdige faser må decisiondate ikke være tidligere enn sluttdato.'
  check: decisiondate >= seneste_stoppmilepael_dato
"""


RULE_CATALOG_SOURCES["milepeler"] = r"""
rule_group: Milepæler
table: milepaeler
database: saksbehandling
description: Datakvalitet på milepæler knyttet til saksfaser
pk_column: pk_milepaeler

# Jeg har valgt å kun se på PB360-saker per nå
where: fagsystem = 'PB360'

# Vi trenger å joine inn faser for å få tak i indikator-kolonnen, som brukes i noen av reglene.
joins:
- table: saksbehandling.faser
  left_on: fk_faser
  right_on: pk_faser
  how: left
  select:
  - indikator

rules:

# Én regel per kolonne, slik at rule_id peker på nøyaktig én feil.
- rule_id: MIL-001
  name: milestone_title mangler
  check: milestone_title IS NOT NULL

- rule_id: MIL-002
  name: fk_faser mangler
  check: fk_faser IS NOT NULL

# Dette er mer en teknisk sjekk
- rule_id: MIL-003
  name: Milepælen kan ikke være nådd uten sluttdato
  description: 'Milepæler som er nådd må ha en sluttdato.'
  when: statusdescription = 'Nådd'
  check: milestonedate IS NOT NULL

# I PB360 er det en del par av milepæler som skal komme sammen. event_flow
# sjekker at de kommer parvis OG i riktig rekkefølge. 
# event_flow-milepælene kan være i en loop hvor som helst i milepælslisten og ubegrenset antall ganger. Man kan ikke ha flere slike sett i samme regel.
# Formålet med completion_gate er å angi en milepæl som må være nådd for at event_flow skal gjelde. 
# Dette er nyttig når man har flere sett med milepæler i samme fase, men hvor man kun ønsker å evaluere dem når den spesifiserte milepælen er nådd, i stedet for å vente på sluttmilepælen.

# Jeg har i disse reglene ikke brukt starts_with og ends_with, men motoren ville da sjekket om disse kom i riktig rekkefølge

- rule_id: MIL-004
  name: Oppstartsmøte, milepæler for tilleggsdokumentasjon må komme parvis
  when: indikator = 'Oppstartsmøte'
  event_flow:
    event_column: milestone_title
    group_column: fk_faser
    order_column: milestonedate
    cycle:
    - Anmodning om oppdatert plandokumentasjon
    - Mottatt oppdatert plandokumentasjon

- rule_id: MIL-005
  name: Offentlig ettersyn, milepæler for tilleggsdokumentasjon må komme parvis
  when: indikator LIKE 'Offentlig ettersyn%'
  event_flow:
    event_column: milestone_title
    group_column: fk_faser
    order_column: milestonedate
    cycle:
    - Anmodning om oppdatert plandokumentasjon
    - Mottatt oppdatert plandokumentasjon
    completion_gate: 
      event_column: milestone_title
      value: Planforslaget er komplett
      order_column: milestonedate
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
