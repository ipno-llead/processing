import pandas as pd
import deba
from lib.uid import gen_uid
from lib.columns import clean_column_names, set_values
from lib.clean import (
    strip_leading_comma,
    clean_dates,
    clean_races,
    clean_sexes,
    clean_names,
    clean_datetime,
    standardize_desc_cols,
)

import pandas as pd

def select_base_columns(df):
    base_cols = [
        'incident_number', 'date',
        'type_of_call', 'officer', 'duty_status', 'suspect_s_race',
        'suspect_s_sex', 'suspect_s_actions'
    ]
    return df[base_cols + [col for col in df.columns if col not in base_cols]]

def summarize_injuries(df):
    employee_cols = ['employee_injury_minor', 'employee_injury_serious', 'employee_injury_death']
    suspect_cols = [
        'suspect_injury_no_complaint', 'suspect_injury_complaint_non_observed',
        'suspect_injury_minor', 'suspect_injury_serious', 'suspect_injury_death'
    ]

    def summarize(row, cols):
        levels = []
        for col in cols:
            val = str(row.get(col)).strip().lower()
            if val.startswith("selected >"):
                levels.append(
                    col.replace('employee_injury_', '')
                       .replace('suspect_injury_', '')
                       .replace('_', ' ')
                )
        return "; ".join(levels) if levels else 'none'

    df['officer_injured'] = df.apply(lambda row: summarize(row, employee_cols), axis=1)
    df['civilian_injured'] = df.apply(lambda row: summarize(row, suspect_cols), axis=1)

    return df

def drop_redundant_columns(df):
    drop_cols = [
        'soft_empty_hand_control', 'soft_empty_hand_control_effect', 
        'hard_empty_hand_control', 'hard_empty_hand_control_effect',
        'oc_spray_foam', 'oc_spray_foam_effect', 'taser', 'taser_effect',
        'impact_weapon', 'impact_weapon_effect', 'impact_weapon_used',
        'impact_munitions', 'impact_munitions_effect', 'impact_munitions_used',
        'chemical_munitions', 'chemical_munitions_effect', 'chemical_munitions_used',
        'diversionary_device', 'diversionary_device_effect',
        'firearm_displayed', 'firearm_displayed_effect',
        'firearm_discharged', 'firearm_discharged_effect',
        'canine', 'canine_effect', 'canine_bite', 'other_force', 'other_effect',
        'other_force_specified',
        'employee_injury_minor', 'employee_injury_serious', 'employee_injury_death',
        'suspect_injury_no_complaint', 'suspect_injury_complaint_non_observed',
        'suspect_injury_minor', 'suspect_injury_serious', 'suspect_injury_death',
        'drive_stun', '21_ft_cartridge', '21_ft_cartridges_fired',
        '25_ft_cartridge', '25_ft_cartridges_fired', 'taser_range',
        'darts_penatrated', 'reason_for_no_penatration',
        'number_of_impact_munitions_used', 'impact_munitions_caliber',
        'number_of_chemical_munitions_used', 'diversionary_device_number_used',
        'firearm_discharged_hit', 'employee_first_aid', 'decontaminated'
    ]
    return df.drop(columns=[col for col in drop_cols if col in df.columns], errors='ignore')


def simplify_use_of_force(df):
    force_cols = {
        "soft_empty_hand_control": "soft_empty_hand_control_effect",
        "hard_empty_hand_control": "hard_empty_hand_control_effect",
        "oc_spray_foam": "oc_spray_foam_effect",
        "taser": "taser_effect",
        "impact_weapon": "impact_weapon_effect",
        "impact_munitions": "impact_munitions_effect",
        "chemical_munitions": "chemical_munitions_effect",
        "diversionary_device": "diversionary_device_effect",
        "firearm_displayed": "firearm_displayed_effect",
        "firearm_discharged": "firearm_discharged_effect",
        "canine": "canine_effect",
        "other_force": "other_effect"
    }

    force_types = []
    force_effects = []

    for _, row in df.iterrows():
        row_types = []
        row_effects = []
        for force, effect in force_cols.items():
            val = str(row.get(force, "")).strip().upper()
            if val.startswith("SELECTED"):
                label = force.replace("_", " ").title()
                row_types.append(label)
                eff_val = str(row.get(effect, "")).strip()
                if eff_val and eff_val.lower() != "nan":
                    row_effects.append(f"{label}: {eff_val}")
        force_types.append(" | ".join(row_types) if row_types else None)
        force_effects.append(" | ".join(row_effects) if row_effects else None)

    df["use_of_force_type"] = force_types
    df["use_of_force_effect_result"] = force_effects
    return df

def clean_call_type(df):
    replacements = {
        "criminal dama": "criminal damage",
        "weapon compl": "weapon complaint",
        "narcotics com": "narcotics complaint",
        "susp person/ci": "suspicious person/circumstance",
        "mental complai": "mental complaint",
        "emergency ass": "emergency assistance",
        "backup agency": "agency backup",
        "attempted suici": "attempted suicide",
        "welfare concer": "welfare concern",
        "traffic complai": "traffic complaint",
        "traaffic stop": "traffic stop",
        "back another a": "backup another agency",
    }

    df["call_type"] = (
        df["call_type"]
        .str.lower()
        .str.strip()
        .replace(replacements)
    )

    return df

def clean_civilian_actions(df):
    replacements = {
        "empty hand defensive resistanc": "empty hand defensive resistance",
        "empty hand active aggression": "empty hand active aggression",
        "verbal resistance/aggression": "verbal resistance/aggression",
        "edged weapon": "edged weapon",
        "firearm": "firearm",
        "cooperative": "cooperative",
        "passive resistance": "passive resistance",
        "none": "none",
        "other": "other",
    }

    def standardize_action(action):
        if not isinstance(action, str):
            return action
        action = action.strip().lower()
        if action.startswith("other:"):
            return action  
        return replacements.get(action, action)

    df["civilian_actions"] = df["civilian_actions"].apply(standardize_action)
    return df

def clean_use_of_force_description(df):
    def standardize_force(val):
        if not isinstance(val, str):
            return val 
        parts = [part.strip() for part in val.split("|")]
        return "; ".join(parts)
    
    df["use_of_force_description"] = df["use_of_force_description"].apply(standardize_force)
    return df

def clean_use_of_force_effective(df):
    def fix_truncation(effect):
        effect = effect.strip()
        effect = effect.replace("Stopped Lev", "Stopped Level of Resistance")
        effect = effect.replace("Reduced Le", "Reduced Level of Resistance")
        effect = effect.replace("No Effect on", "No Effect on Level of Resistance")
        effect = effect.replace("No Effect on Level of Resista", "No Effect on Level of Resistance")
        effect = effect.replace("Stopped Level of", "Stopped Level of Resistance")
        effect = effect.replace("Reduced Level of Resista", "Reduced Level of Resistance")
        effect = effect.replace("Stopped Level of Resistan", "Stopped Level of Resistance")
        effect = effect.replace("Reduced Level of Resistanc", "Reduced Level of Resistance")
        effect = effect.replace("No Effect on Level of Resistanc", "No Effect on Level of Resistance")
        return effect

    def standardize_entry(entry):
        if not isinstance(entry, str):
            return entry
        parts = [fix_truncation(p) for p in entry.split("|")]
        cleaned = "; ".join(p.strip() for p in parts)
        return cleaned

    df["use_of_force_effective"] = df["use_of_force_effective"].apply(standardize_entry)
    return df

def split_officer_name(df):
    df[["last_name", "first_name"]] = df["officer_name"].str.strip().str.lower().str.split(" ", n=1, expand=True)
    df = df.drop(columns=["officer_name"])
    return df


def clean():
    df = (pd.read_csv(deba.data("raw/lake_charles_pd/lake_charles_pd_uof_2020.csv"))
        .pipe(clean_column_names)
        .drop(columns=["use_of_force", "day_of_the_week", "time", "taser_model"])
        .pipe(select_base_columns)
        .pipe(simplify_use_of_force)
        .pipe(summarize_injuries)
        .pipe(drop_redundant_columns)
        .rename(
            columns={
                "incident_number": "tracking_id_og",
                "date": "incident_date",
                "type_of_call": "call_type",
                "officer": "officer_name",
                "duty_status": "officer_duty_status",
                "suspect_s_race": "civilian_race",
                "suspect_s_sex": "civilian_sex",
                "suspect_s_actions": "civilian_actions",
                "use_of_force_type": "use_of_force_description",
                "use_of_force_effect_result": "use_of_force_effective",
                })
        .pipe(clean_dates, ["incident_date"])
        .pipe(clean_races, ["civilian_race"])
        .pipe(clean_sexes, ["civilian_sex"])
        .pipe(clean_names, ["officer_name"])
        .pipe(standardize_desc_cols, ["call_type", "officer_duty_status", "use_of_force_description", "use_of_force_effective"])
        .pipe(clean_call_type)
        .pipe(clean_civilian_actions)
        .pipe(clean_use_of_force_description)
        .pipe(clean_use_of_force_effective)
        .pipe(split_officer_name)
        .pipe(set_values, {"agency": "lake-charles-pd"})
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
        .pipe(gen_uid, ["tracking_id_og", "agency"], "tracking_id")
        .pipe(gen_uid, ["tracking_id", "first_name", "last_name", "agency"], "uof_uid")
    )
    return df 


if __name__ == "__main__":
    uof = clean()
    uof.to_csv(deba.data("clean/uof_lake_charles_pd_2020.csv"), index=False)
