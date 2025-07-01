
import mysql.connector
from mysql.connector import Error
import pandas as pd
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
import logging
from config import DB_CONFIG, DEPARTMENT_SALARIES, WORK_HOURS_PER_YEAR

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manage database operations for WTL system"""
    
    def __init__(self, config: Dict[str, str] = DB_CONFIG):
        self.config = config
        self.connection = None
        self.cursor = None
    
    def connect(self):
        """Establish database connection"""
        try:
            self.connection = mysql.connector.connect(**self.config)
            self.cursor = self.connection.cursor()
            logger.info("Successfully connected to MySQL database")
        except Error as e:
            logger.error(f"Error connecting to MySQL: {e}")
            raise
    
    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("Database connection closed")
    
    def create_database(self):
        """Create database if it doesn't exist"""
        try:
            # Connect without specifying database
            temp_config = self.config.copy()
            temp_config.pop('database', None)
            temp_conn = mysql.connector.connect(**temp_config)
            temp_cursor = temp_conn.cursor()
            
            # Create database
            temp_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.config['database']}")
            logger.info(f"Database '{self.config['database']}' created or already exists")
            
            temp_cursor.close()
            temp_conn.close()
        except Error as e:
            logger.error(f"Error creating database: {e}")
            raise
    
    def create_tables(self):
        """Create all necessary tables"""
        try:
            # Financial summary table for automated reports
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS financial_summary (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    report_date DATE NOT NULL,
                    project_code VARCHAR(50) NOT NULL,
                    project_name TEXT,
                    project_type VARCHAR(10),
                    status VARCHAR(20),
                    contract_price DECIMAL(15, 2),
                    purchase_cost DECIMAL(15, 2),
                    labor_cost DECIMAL(15, 2),
                    total_cost DECIMAL(15, 2),
                    profit DECIMAL(15, 2),
                    profit_margin DECIMAL(5, 2),
                    total_hours DECIMAL(10, 2),
                    efficiency_score DECIMAL(10, 2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_report_date (report_date),
                    INDEX idx_project_code (project_code)
                )
            """)
            
            # Department summary table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS department_summary (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    report_date DATE NOT NULL,
                    department_name VARCHAR(50) NOT NULL,
                    total_hours DECIMAL(10, 2),
                    total_labor_cost DECIMAL(15, 2),
                    num_projects INT,
                    num_tasks INT,
                    avg_hourly_rate DECIMAL(10, 2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_report_date (report_date),
                    INDEX idx_department (department_name)
                )
            """)
            
            # Automated reports log
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS report_log (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    report_type VARCHAR(50),
                    report_date DATE,
                    file_path TEXT,
                    summary JSON,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_report_type (report_type),
                    INDEX idx_created_at (created_at)
                )
            """)
            
            self.connection.commit()
            logger.info("All tables created successfully")
            
        except Error as e:
            logger.error(f"Error creating tables: {e}")
            raise
    
    def insert_financial_summary(self, summary_df: pd.DataFrame, report_date: str = None):
        """Insert financial summary data into database"""
        try:
            if report_date is None:
                report_date = datetime.now().strftime('%Y-%m-%d')
            
            # Prepare data for insertion
            insert_query = """
                INSERT INTO financial_summary 
                (report_date, project_code, project_name, project_type, status,
                 contract_price, purchase_cost, labor_cost, total_cost,
                 profit, profit_margin, total_hours, efficiency_score)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            data_to_insert = []
            for _, row in summary_df.iterrows():
                data_to_insert.append((
                    report_date,
                    row['ProjectCode'],
                    row['ProjectName'],
                    row['ProjectType'],
                    row.get('Status', 'Unknown'),
                    float(row['ContractPrice']),
                    float(row['PurchaseCost']),
                    float(row.get('LaborCost', 0)),
                    float(row['TotalCost']),
                    float(row['Profit']),
                    float(row['ProfitMargin']),
                    float(row.get('TotalHours', 0)),
                    float(row.get('EfficiencyScore', 0))
                ))
            
            self.cursor.executemany(insert_query, data_to_insert)
            self.connection.commit()
            
            logger.info(f"Inserted {len(data_to_insert)} records into financial_summary")
            
        except Error as e:
            logger.error(f"Error inserting financial summary: {e}")
            self.connection.rollback()
            raise
    
    def insert_department_summary(self, dept_summary_df: pd.DataFrame, report_date: str = None):
        """Insert department summary data into database"""
        try:
            if report_date is None:
                report_date = datetime.now().strftime('%Y-%m-%d')
            
            insert_query = """
                INSERT INTO department_summary 
                (report_date, department_name, total_hours, total_labor_cost,
                 num_projects, num_tasks, avg_hourly_rate)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            data_to_insert = []
            for dept, row in dept_summary_df.iterrows():
                data_to_insert.append((
                    report_date,
                    dept,
                    float(row['TotalHours']),
                    float(row['TotalLaborCost']),
                    int(row['NumProjects']),
                    int(row['NumTasks']),
                    float(row['HourlyRate'])
                ))
            
            self.cursor.executemany(insert_query, data_to_insert)
            self.connection.commit()
            
            logger.info(f"Inserted {len(data_to_insert)} department records")
            
        except Error as e:
            logger.error(f"Error inserting department summary: {e}")
            self.connection.rollback()
            raise
    
    def log_report(self, report_type: str, file_path: str, summary: Dict[str, Any]):
        """Log generated report in database"""
        try:
            insert_query = """
                INSERT INTO report_log (report_type, report_date, file_path, summary)
                VALUES (%s, %s, %s, %s)
            """
            
            report_date = datetime.now().strftime('%Y-%m-%d')
            summary_json = json.dumps(summary, ensure_ascii=False)
            
            self.cursor.execute(insert_query, (report_type, report_date, file_path, summary_json))
            self.connection.commit()
            
            logger.info(f"Logged {report_type} report")
            
        except Error as e:
            logger.error(f"Error logging report: {e}")
            self.connection.rollback()
            raise
    
    def generate_automated_report(self, start_date: str = None, end_date: str = None) -> Dict:
        """Generate automated report from database"""
        try:
            if start_date is None:
                start_date = datetime.now().strftime('%Y-%m-01')
            if end_date is None:
                end_date = datetime.now().strftime('%Y-%m-%d')
            
            report = {
                'report_period': f"{start_date} to {end_date}",
                'generated_at': datetime.now().isoformat(),
                'summary': {},
                'trends': {},
                'alerts': []
            }
            
            # Overall summary
            summary_query = """
                SELECT 
                    COUNT(DISTINCT project_code) as total_projects,
                    SUM(contract_price) as total_revenue,
                    SUM(total_cost) as total_cost,
                    SUM(profit) as total_profit,
                    AVG(profit_margin) as avg_profit_margin,
                    SUM(total_hours) as total_hours
                FROM financial_summary
                WHERE report_date BETWEEN %s AND %s
            """
            
            self.cursor.execute(summary_query, (start_date, end_date))
            result = self.cursor.fetchone()
            
            if result:
                report['summary'] = {
                    'total_projects': int(result[0]) if result[0] else 0,
                    'total_revenue': float(result[1]) if result[1] else 0,
                    'total_cost': float(result[2]) if result[2] else 0,
                    'total_profit': float(result[3]) if result[3] else 0,
                    'avg_profit_margin': float(result[4]) if result[4] else 0,
                    'total_hours': float(result[5]) if result[5] else 0
                }
            
            # Project type breakdown
            type_query = """
                SELECT 
                    project_type,
                    COUNT(*) as count,
                    SUM(profit) as total_profit,
                    AVG(profit_margin) as avg_margin
                FROM financial_summary
                WHERE report_date BETWEEN %s AND %s
                GROUP BY project_type
            """
            
            self.cursor.execute(type_query, (start_date, end_date))
            type_results = self.cursor.fetchall()
            
            report['project_types'] = {}
            for row in type_results:
                report['project_types'][row[0]] = {
                    'count': int(row[1]),
                    'total_profit': float(row[2]),
                    'avg_margin': float(row[3])
                }
            
            # Status breakdown (GS projects)
            status_query = """
                SELECT 
                    status,
                    COUNT(*) as count,
                    AVG(profit_margin) as avg_margin
                FROM financial_summary
                WHERE report_date BETWEEN %s AND %s
                    AND project_type = 'GS'
                    AND status != 'Unknown'
                GROUP BY status
            """
            
            self.cursor.execute(status_query, (start_date, end_date))
            status_results = self.cursor.fetchall()
            
            report['project_status'] = {}
            for row in status_results:
                report['project_status'][row[0]] = {
                    'count': int(row[1]),
                    'avg_margin': float(row[2])
                }
            
            # Department performance
            dept_query = """
                SELECT 
                    department_name,
                    SUM(total_hours) as total_hours,
                    SUM(total_labor_cost) as total_cost,
                    AVG(num_projects) as avg_projects
                FROM department_summary
                WHERE report_date BETWEEN %s AND %s
                GROUP BY department_name
                ORDER BY total_hours DESC
                LIMIT 10
            """
            
            self.cursor.execute(dept_query, (start_date, end_date))
            dept_results = self.cursor.fetchall()
            
            report['top_departments'] = []
            for row in dept_results:
                report['top_departments'].append({
                    'department': row[0],
                    'total_hours': float(row[1]),
                    'total_cost': float(row[2]),
                    'avg_projects': float(row[3])
                })
            
            # Generate alerts
            # Alert 1: Loss-making projects
            loss_query = """
                SELECT COUNT(*) as count, SUM(profit) as total_loss
                FROM financial_summary
                WHERE report_date BETWEEN %s AND %s AND profit < 0
            """
            
            self.cursor.execute(loss_query, (start_date, end_date))
            loss_result = self.cursor.fetchone()
            
            if loss_result and loss_result[0] > 0:
                report['alerts'].append({
                    'type': 'loss_making_projects',
                    'severity': 'high',
                    'message': f"{loss_result[0]} projects with total loss of ¥{abs(loss_result[1]):,.2f}"
                })
            
            # Alert 2: Low efficiency
            low_eff_query = """
                SELECT COUNT(*) as count
                FROM financial_summary
                WHERE report_date BETWEEN %s AND %s 
                    AND efficiency_score < 100 
                    AND total_hours > 0
            """
            
            self.cursor.execute(low_eff_query, (start_date, end_date))
            eff_result = self.cursor.fetchone()
            
            if eff_result and eff_result[0] > 0:
                report['alerts'].append({
                    'type': 'low_efficiency',
                    'severity': 'medium',
                    'message': f"{eff_result[0]} projects with low efficiency scores"
                })
            
            return report
            
        except Error as e:
            logger.error(f"Error generating automated report: {e}")
            raise


class AutomatedReportGenerator:
    """Generate automated reports using database data"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
    
    def generate_daily_report(self) -> str:
        """Generate daily summary report"""
        report = self.db.generate_automated_report()
        
        # Format report
        lines = []
        lines.append("=" * 60)
        lines.append("WTL DAILY FINANCIAL REPORT")
        lines.append("=" * 60)
        lines.append(f"Generated: {report['generated_at']}")
        lines.append(f"Period: {report['report_period']}")
        lines.append("")
        
        # Summary
        summary = report['summary']
        lines.append("SUMMARY")
        lines.append("-" * 30)
        lines.append(f"Total Projects: {summary['total_projects']}")
        lines.append(f"Total Revenue: ¥{summary['total_revenue']:,.2f}")
        lines.append(f"Total Profit: ¥{summary['total_profit']:,.2f}")
        lines.append(f"Avg Profit Margin: {summary['avg_profit_margin']:.2f}%")
        lines.append("")
        
        # Alerts
        if report['alerts']:
            lines.append("ALERTS")
            lines.append("-" * 30)
            for alert in report['alerts']:
                lines.append(f"[{alert['severity'].upper()}] {alert['message']}")
            lines.append("")
        
        return '\n'.join(lines)


def main():
    """Test database operations"""
    db = DatabaseManager()
    
    try:
        # Setup database
        db.create_database()
        db.connect()
        db.create_tables()
        
        # Test with sample data
        from data_loader import DataLoader
        from data_processor import DataProcessor
        
        # Load and process data
        loader = DataLoader()
        work_hours, gs_projects, iss_projects = loader.load_all_data()
        combined_projects = loader.combine_projects()
        
        processor = DataProcessor(work_hours, combined_projects)
        financial_summary = processor.calculate_all_metrics()
        
        # Insert data
        db.insert_financial_summary(financial_summary)
        db.insert_department_summary(processor.department_summary_df)
        
        # Generate automated report
        report = db.generate_automated_report()
        print("\nAutomated Report:")
        print(json.dumps(report, indent=2, ensure_ascii=False))
        
        # Log report
        db.log_report('daily_summary', 'reports/daily_summary.txt', report['summary'])
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise
    
    finally:
        db.disconnect()


if __name__ == "__main__":
    main()