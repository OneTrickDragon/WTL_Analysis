# WTL_Analysis
Overview
This comprehensive financial analysis system processes WTL Design's Q3 20X2 data to provide insights into project profitability, department efficiency, and resource allocation. The system includes automated data processing, MySQL database integration, and advanced visualization capabilities.

# System Architecture
WTL_Analysis/  
├── config.py              # Configuration and constants  
├── data_loader.py         # Excel data loading  
├── data_processor.py      # Data processing and calculations  
├── visualizer.py          # Visualization generation  
├── report_generator.py    # Report generation            
├── database_manager.py    # Database operations        
├── main.py               # Main orchestrator   
 
# 1. Prerequisites

Python 3.8 or higher
MySQL Server 8.0+ (optional, only if using database features)
Excel file: WTL Design Jr. Analyst Task.xlsx

If using database features, update config.py:
pythonDB_CONFIG = {
    'host': 'localhost',
    'user': 'your_username',
    'password': 'your_password',
    'database': 'wtl_financial_db'
}

Update path of the excel file in config.py accordingly:
EXCEL_FILE_PATH = r'C:\Users\arjun\Downloads\WTL Design Jr. Analyst Task.xlsx'

# Run complete analysis or Specify custom Excel file
python main.py

python main.py --excel "path/to/your/file.xlsx"

Run analysis and store in database:
python main.py --use-db

# Output Files
Visualizations (HTML)
Located in visualizations/ directory:

main_dashboard.html - Comprehensive financial dashboard
profitability_analysis.html - Detailed profit analysis
efficiency_analysis.html - Project efficiency metrics
department_analysis.html - Department performance
project_status_analysis.html - GS project status breakdown
time_series_analysis.html - Temporal patterns

Reports (Text/JSON)
Located in reports/ directory:

q3_financial_report.txt - Comprehensive text report
executive_summary.json - Key metrics and findings
department_report.txt - Department-specific analysis
gs_status_report.txt - GS project status details
strategic_recommendations.txt - Action items
automated_daily_report.txt - Database-generated report (if --use-db)

