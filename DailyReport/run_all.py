from dailyReportBatteryV2 import main
import pandas as pd

# Generate the array of dates
report_dates = pd.date_range(start="2025-02-12", end="2025-02-20")

input_path = 'C:/Users/ValentinMulet/Code/DailyReport/tftp_root_decoded/'
pdf_path = 'C:/Users/ValentinMulet/Code/'

for report_date in report_dates:
    pdf_name = pdf_path + f'DailyEnergyReport-{report_date.strftime("%Y_%m_%d")}.pdf'
    main(report_date.strftime('%Y-%m-%d'), input_path, pdf_name)