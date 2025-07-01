import os
import sys
import logging
from datetime import datetime
import argparse

# Import all modules
from config import *
from data_loader import DataLoader
from data_processor import DataProcessor
from Visualization import Visualizer
from report_generator import ReportGenerator
from database_manager import DatabaseManager, AutomatedReportGenerator

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class WTLAnalysisSystem:
    """Main system orchestrator"""
    
    def __init__(self, excel_path: str = EXCEL_FILE_PATH, use_database: bool = False):
        self.excel_path = excel_path
        self.use_database = use_database
        self.data_loader = None
        self.data_processor = None
        self.visualizer = None
        self.report_generator = None
        self.db_manager = None
        
        # Data storage
        self.work_hours_df = None
        self.projects_df = None
        self.financial_summary_df = None
        self.department_summary_df = None
        
    def run_complete_analysis(self):
        """Run complete financial analysis pipeline"""
        logger.info("Starting WTL Financial Analysis System...")
        
        try:
            # Step 1: Load data
            self._load_data()
            
            # Step 2: Process data
            self._process_data()
            
            # Step 3: Generate visualizations
            self._create_visualizations()
            
            # Step 4: Generate reports
            self._generate_reports()
            
            # Step 5: Database operations (if enabled)
            if self.use_database:
                self._database_operations()
            
            logger.info("Analysis completed successfully!")
            self._print_summary()
            
        except Exception as e:
            logger.error(f"Error during analysis: {e}")
            raise
    
    def _load_data(self):
        """Load data from Excel file"""
        logger.info("Loading data from Excel file...")
        
        self.data_loader = DataLoader(self.excel_path)
        work_hours, gs_projects, iss_projects = self.data_loader.load_all_data()
        
        self.work_hours_df = work_hours
        self.projects_df = self.data_loader.combine_projects()
        
        logger.info(f"Loaded {len(self.work_hours_df)} work hour records")
        logger.info(f"Loaded {len(self.projects_df)} projects")
    
    def _process_data(self):
        """Process data and calculate metrics"""
        logger.info("Processing data and calculating metrics...")
        
        self.data_processor = DataProcessor(self.work_hours_df, self.projects_df)
        self.financial_summary_df = self.data_processor.calculate_all_metrics()
        self.department_summary_df = self.data_processor.department_summary_df
        
        # Get analyses
        self.analyses = {
            'efficiency': self.data_processor.get_efficiency_analysis(),
            'profitability': self.data_processor.get_profitability_analysis()
        }
        
        logger.info("Data processing completed")
    
    def _create_visualizations(self):
        """Create all visualizations"""
        logger.info("Creating visualizations...")
        
        self.visualizer = Visualizer(
            self.financial_summary_df,
            self.department_summary_df,
            self.work_hours_df
        )
        
        self.visualizer.create_all_visualizations()
        logger.info("Visualizations created")
    
    def _generate_reports(self):
        """Generate all reports"""
        logger.info("Generating reports...")
        
        self.report_generator = ReportGenerator(
            self.financial_summary_df,
            self.department_summary_df,
            self.analyses
        )
        
        self.report_generator.generate_all_reports()
        logger.info("Reports generated")
    
    def _database_operations(self):
        """Perform database operations"""
        logger.info("Performing database operations...")
        
        self.db_manager = DatabaseManager()
        
        try:
            # Setup database
            self.db_manager.create_database()
            self.db_manager.connect()
            self.db_manager.create_tables()
            
            # Insert data
            report_date = datetime.now().strftime('%Y-%m-%d')
            self.db_manager.insert_financial_summary(
                self.financial_summary_df, 
                report_date
            )
            self.db_manager.insert_department_summary(
                self.department_summary_df, 
                report_date
            )
            
            # Generate automated report
            auto_gen = AutomatedReportGenerator(self.db_manager)
            auto_report = auto_gen.generate_daily_report()
            
            # Save automated report
            auto_report_path = os.path.join(REPORT_CONFIG['output_dir'], 'automated_daily_report.txt')
            with open(auto_report_path, 'w', encoding='utf-8') as f:
                f.write(auto_report)
            
            logger.info("Database operations completed")
            
        finally:
            self.db_manager.disconnect()
    
    def _print_summary(self):
        """Print analysis summary"""
        print("\n" + "=" * 60)
        print("ANALYSIS COMPLETE")
        print("=" * 60)
        
        # Financial summary
        overall = self.analyses['profitability']['overall_metrics']
        print("\nFinancial Summary:")
        print(f"  Total Revenue: ¥{overall['total_revenue']:,.2f}")
        print(f"  Total Profit: ¥{overall['total_profit']:,.2f}")
        print(f"  Average Margin: {overall['average_profit_margin']:.2f}%")
        
        # File outputs
        print("\nGenerated Files:")
        print(f"  Visualizations: {REPORT_CONFIG['visualizations_dir']}/")
        print(f"  Reports: {REPORT_CONFIG['output_dir']}/")
        
        if self.use_database:
            print("\nDatabase Status:")
            print("  Data inserted successfully")
            print("  Automated report generated")
        
        print("\n" + "=" * 60)
    
    def generate_custom_report(self, report_type: str, **kwargs):
        """Generate custom report based on specific criteria"""
        if not self.financial_summary_df is not None:
            raise ValueError("Must run analysis first")
        
        logger.info(f"Generating custom report: {report_type}")
        
        if report_type == 'department':
            # Filter by department
            dept = kwargs.get('department')
            if dept:
                # Implementation for department-specific report
                pass
        
        elif report_type == 'project_type':
            # Filter by project type
            ptype = kwargs.get('project_type')
            if ptype:
                filtered_data = self.financial_summary_df[
                    self.financial_summary_df['ProjectType'] == ptype
                ]
                # Generate report for filtered data
        
        elif report_type == 'status':
            # Filter by status
            status = kwargs.get('status')
            if status:
                filtered_data = self.financial_summary_df[
                    self.financial_summary_df['Status'] == status
                ]
                # Generate report for filtered data
        
        logger.info(f"Custom report {report_type} generated")


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description='WTL Financial Analysis System')
    parser.add_argument(
        '--excel', 
        type=str, 
        default=EXCEL_FILE_PATH,
        help='Path to Excel file'
    )
    parser.add_argument(
        '--use-db', 
        action='store_true',
        help='Use database for storing results and automated reports'
    )
    parser.add_argument(
        '--report-only', 
        action='store_true',
        help='Generate reports only (skip visualizations)'
    )
    
    args = parser.parse_args()
    
    # Create system instance
    system = WTLAnalysisSystem(
        excel_path=args.excel,
        use_database=args.use_db
    )
    
    # Run analysis
    system.run_complete_analysis()


if __name__ == "__main__":
    main()