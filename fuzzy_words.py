import csv
from enum import Enum
import pandas as pd


class FuzzyWords(Enum):
    DIFFERENTIAL_DIAGNOSIS = "אבחנה מבדלת"
    CURRENT_DIAGNOSIS = "אבחנה נוכחית"
    SUMMARY = "סיכום"
    MEDICAL_BACKGROUND = "רקע רפואי"
    CURRENT_DISEASE = "מחלה נוכחית"
    PLAN = "תוכנית"
    BACKGROUND = "ברקע"
    REPETATIVE_ADMISSIONS = "אישפוזים רצנטיים"
    "במיון"
    IN_DEPARTMENT = "במחלקה"
    SIDE_CHECKS = "בדיקות עזר"
    BACKGROUND_DISEASE = "מחלות רקע"
    IN_LAB = "במעבדה"
    DOCTOR_SUMMARY = "סיכום רופא"
    PHYSICAL_EXAMINATION = "בדיקה גופנית"
    VITAL_SIGNS = "סימנים חיוניים"


df = pd.read_csv('output1.csv', encoding='utf-8', error_bad_lines=False, quoting=csv.QUOTE_NONE)
df.to_csv('new_data12.csv', encoding='utf-8-sig')
