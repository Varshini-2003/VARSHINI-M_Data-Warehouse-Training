import pandas as pd

# === Load Week‑4 cleaned data files ===
students   = pd.read_csv('students.csv')
courses    = pd.read_csv('courses.csv')
enrollments = pd.read_csv('enrollments.csv')
progress   = pd.read_csv('progress.csv')

# === Consolidate the data ===
df = (enrollments
      .merge(students,  on='student_id')
      .merge(courses,   on='course_id')
      .merge(progress,  on=['student_id','course_id'])
      .rename(columns={'progress':'completion_percent'}))

# === Filter for < 50% progress ===
low_progress_df = df[df['completion_percent'] < 50]

# === Save the weekly report ===
low_progress_df.to_csv('weekly_low_progress_report.csv', index=False)
print("Report saved as weekly_low_progress_report.csv")
