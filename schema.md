## File data/fuse/personnel.csv

### Schema

- **uid**:
  - Description: unique identifier for officer

  - Attributes:
    - Unique
    - No NA

- **last_name**:
  - Description: person last name plus suffix if any

  - Attributes:
    - Title case

- **middle_name**:
  - Description: person middle name

  - Attributes:
    - Title case

- **middle_initial**:
  - Description: initial letter of middle name

  - Attributes:
    - Title case

- **first_name**:
  - Description: person first name

  - Attributes:
    - Title case

- **birth_year**:
  - Attributes:
    - Integer
    - Range: `1900` -> `2010`

- **birth_month**:
  - Attributes:
    - Integer
    - Range: `1` -> `12`

- **birth_day**:
  - Attributes:
    - Integer
    - Range: `1` -> `31`

- **race**:
  - Attributes:
    - Options:
      - asian / pacific islander
      - black
      - hispanic
      - white
      - native american

- **sex**:
  - Attributes:
    - Options:
      - male
      - female

## File data/fuse/complaint.csv

### Schema

- **uid**:
  - Description: accused officer's unique identifier. This references the `uid` column in personnel.csv

- **tracking_number**:
  - Description: complaint tracking number from the agency the data originate from

- **complaint_uid**:
  - Description: complaint unique identifier

  - Attributes:
    - Unique
    - No NA

- **allegation_finding**:
  - Description: finding of an individual allegation

  - Attributes:
    - Options:
      - illegitimate outcome
      - pending
      - not sustained
      - counseling
      - exonerated
      - mediation
      - sustained
      - no investigation merited
      - unfounded

- **allegation**:
  - Description: the same data that can be found in `paragraph_code` and `paragraph_violation`. In fact this column should be merged with the paragraph_ columns.

- **allegation_class**:
  - Description: the same data that can be found in `rule_code` and `rule_violation`. This should eventually be merged with the code_ columns.

- **citizen**:
  - Description: demographic info of the involved citizen

- **disposition**:
  - Description: the final disposition for the complaint

- **rule_code**:
  - Description: a charge is usually consist of a rule and paragraph that was violated. This is the code of the violated rule.

- **rule_violation**:
  - Description: a charge is usually consist of a rule and paragraph that was violated. This is the text of the violated rule.

- **paragraph_code**:
  - Description: a charge is usually consist of a rule and paragraph that was violated. This is the paragraph number.

- **paragraph_violation**:
  - Description: a charge is usually consist of a rule and paragraph that was violated. This is the text of the violated paragraph.

- **charges**:
  - Description: `rule_*` and `paragraph_*` columns combined. Inputs that are too hard to parse and split into rule and paragraph will end up here.

- **complainant_name**:
  - Description: redacted if the complainant isn't a police officer.

- **complainant_type**:
  - Description: whether the complaint originate from within the agency or from outside

- **complainant_sex**:
  - Attributes:
    - Options:
      - male
      - female

- **complainant_race**:
  - Attributes:
    - Options:
      - asian / pacific islander
      - black
      - hispanic
      - white
      - native american

- **action**:
  - Description: what (disciplinary) actions were taken regarding this complaint

- **data_production_year**:
  - Description: the year of the original data. Usually it is the year number appear in the original data file. Perhaps we don't need this column anymore (now that there is the event tables).

  - Attributes:
    - Integer
    - Range: `1900` -> `2021`

- **agency**:
  - Description: the police department the accused belong to.

- **incident_type**:
  - Description: This field comes from IPM and seems to contains the same kind of info as `complainant_type`. However it contains conflicting info in some places, we should investigate more.

- **supervisor_uid**:
  - Description: supervisor is usually the investigating supervisor. If the supervisor could be matched against one of the officers in the same department then this references the row with the same `uid` in personnel table.

- **badge_no**:
  - Description: accused's badge number during incident

- **department_code**:
  - Description: internal code of the department the accused belonged to during the incident. Due to how widely unavailable this field is, it is not very useful.

- **department_desc**:
  - Description: text description of the department the accused belonged to during the incident.

- **rank_desc**:
  - Description: text description of the accused's rank during the incident.

- **employment_status**:
  - Description: whether the accused was fulltime or reserve during incident.

### Other characteristics

- if `allegation_finding` is "sustained" then `disposition` should also be "sustained"

## File data/fuse/event.csv

### Schema

- **event_uid**:
  - Description: unique identifier for each event

  - Attributes:
    - Unique
    - No NA

- **kind**:
  - Attributes:
    - Options:
      - appeal_render
      - officer_level_1_cert
      - uof_completed
      - appeal_file
      - officer_pay_effective
      - uof_due
      - investigation_start
      - uof_created
      - officer_rank
      - uof_assigned
      - officer_pc_12_qualification
      - uof_incident
      - suspension_start
      - suspension_end
      - officer_left
      - allegation_create
      - uof_receive
      - appeal_hearing
      - complaint_receive
      - complaint_incident
      - officer_pay_prog_start
      - investigation_complete
      - officer_hire
      - officer_dept
      - award_receive
    - No NA

- **year**:
  - Attributes:
    - Integer
    - No NA
    - Range: `1900` -> `2021`

- **month**:
  - Attributes:
    - Integer
    - Range: `1` -> `12`

- **day**:
  - Attributes:
    - Integer
    - Range: `1` -> `31`

- **time**:
  - Attributes:
    <li>Match regexp: <code>^\d{2}:\d{2}$</code></li>

- **raw_date**:
  - Description: raw date string from which `year`, `month`, `day` and `time` was extracted.

- **uid**:
  - Description: officer unique identifer, references the `uid` column in personnel.csv

- **complaint_uid**:
  - Description: complaint unique identifier, refernces the `complaint_uid` column in complaint.csv

- **appeal_uid**:
  - Description: appeal hearing unique identifier, references the `appeal_uid` column in app_*.csv files.

- **uof_uid**:
  - Description: unique identifier for a use of force report, references the `uof_uid` column in use_of_force.csv.

- **agency**:
  - Description: the police department that the officer and/or complaint/use of force report referred to belong to.

- **badge_no**:
  - Description: officer's badge number

- **employee_id**:
  - Description: referred officer's internal employee number.

- **department_code**:
  - Description: internal code of the department the officer belong to at the time of the event. Due to how widely unavailable this field is, it is not very useful.

- **department_desc**:
  - Description: text description of the department the officer belong to at the time of the event.

- **rank_code**:
  - Description: internal rank code of the officer at the time of the event. Due to how widely unavailable this field is, it is not very useful.

- **rank_desc**:
  - Description: text description of the officer's rank at the time of the event.

- **employee_class**:
  - Description: classification of job titles

- **employment_status**:
  - Description: whether the officer is full time, part time, reserve, ... etc

- **sworn**:
  - Description: whether the officer is sworn or not

- **officer_inactive**:
  - Description: whether the officer is still active

- **employee_type**:
  - Description: whether the officer is commissioned or not.

- **years_employed**:
  - Attributes:
    - Integer

- **salary**:
  - Attributes:
    - Float

- **salary_freq**:
  - Attributes:
    - Options:
      - yearly
      - monthly
      - weekly
      - hourly
      - daily
      - bi-weekly

- **award**:
  - Description: award given to the officer

- **award_comments**:
  - Description: further description of the award

### Other characteristics

- no officer with more than 1 left date in a calendar month

## File data/fuse/use_of_force.csv

### Schema

- **uof_uid**:
  - Description: unique identifier of use of force report.

  - Attributes:
    - Unique
    - No NA

- **uof_tracking_number**:
  - Description: tracking number of use of force report from the originating agency.

- **report_year**:
  - Description: year of report

- **uid**:
  - Description: officer unique identifier, this references the `uid` column from personnel.csv

- **force_description**:
  - Description: summary of force used

- **citizen_uid**:
  - Description: unique identifier for each citizen involved in a report.

- **citizen_age**:
  - Attributes:
    - Integer

- **citizen_sex**:
  - Attributes:
    - Options:
      - male
      - female

- **citizen_race**:
  - Attributes:
    - Options:
      - asian / pacific islander
      - black
      - hispanic
      - white
      - native american

- **citizen_age_1**:
  - Attributes:
    - Integer

- **officer_age**:
  - Attributes:
    - Integer

- **officer_years_exp**:
  - Attributes:
    - Integer

- **officer_years_with_unit**:
  - Attributes:
    - Integer

- **data_production_year**:
  - Description: the year of the original data. Usually it is the year number appear in the original data file. Perhaps we don't need this column anymore (now that there is the event tables).

  - Attributes:
    - Integer
    - Range: `1900` -> `2021`

- **agency**:
  - Description: the police department this report originate from.
