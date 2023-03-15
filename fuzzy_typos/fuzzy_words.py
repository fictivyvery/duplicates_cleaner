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
    EMERGENCY_DEPARTMENT = "במיון"
    IN_DEPARTMENT = "במחלקה"
    SIDE_CHECKS = "בדיקות עזר"
    BACKGROUND_DISEASE = "מחלות רקע"
    IN_LAB = "במעבדה"
    DOCTOR_SUMMARY = "סיכום רופא"
    PHYSICAL_EXAMINATION = "בדיקה גופנית"
    VITAL_SIGNS = "סימנים חיוניים"


