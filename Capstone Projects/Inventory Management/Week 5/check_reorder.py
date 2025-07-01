import pandas as pd

# Load the inventory CSV file
df = pd.read_csv("final_inventory_dashboard.csv")

# Filter only products that need reorder
reorder_df = df[df["reorder_flag"] == "REORDER"]

# Save the result to a new CSV file
reorder_df.to_csv("reorder_report.csv", index=False)

print("reorder_report.csv generated successfully.")