import pandas as pd
import numpy as np
from datetime import datetime
import os
import json
from typing import Dict, List, Any
import logging
from config import REPORT_CONFIG, DEPARTMENT_SALARIES

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ReportGenerator:
    """Generate comprehensive reports"""
    
    def __init__(self, financial_summary_df: pd.DataFrame,
                 department_summary_df: pd.DataFrame,
                 processor_analyses: Dict[str, Any]):
        self.financial_summary = financial_summary_df
        self.department_summary = department_summary_df
        self.analyses = processor_analyses
        
        # Create output directory
        self.output_dir = REPORT_CONFIG['output_dir']
        os.makedirs(self.output_dir, exist_ok=True)
        
    def generate_all_reports(self):
        """Generate all report types"""
        logger.info("Generating reports...")
        
        # Text report
        self.generate_text_report()
        
        # Executive summary
        self.generate_executive_summary()
        
        # Department report
        self.generate_department_report()
        
        # Project status report
        self.generate_status_report()
        
        # Recommendations report
        self.generate_recommendations()
        
        logger.info(f"All reports saved to {self.output_dir}")
    
    def generate_text_report(self):
        """Generate comprehensive text report"""
        report_lines = []
        
        # Header
        report_lines.extend([
            "=" * 80,
            "WTL DESIGN Q3 20X2 FINANCIAL ANALYSIS REPORT",
            "=" * 80,
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "=" * 80,
            "EXECUTIVE SUMMARY",
            "=" * 80,
            ""
        ])
        
        # Overall metrics
        overall = self.analyses['profitability']['overall_metrics']
        report_lines.extend([
            f"Total Revenue: ¥{overall['total_revenue']:,.2f}",
            f"Total Cost: ¥{overall['total_cost']:,.2f}",
            f"Total Profit: ¥{overall['total_profit']:,.2f}",
            f"Average Profit Margin: {overall['average_profit_margin']:.2f}%",
            f"Total Projects Analyzed: {len(self.financial_summary)}",
            f"  - Profitable: {overall['profitable_projects_count']}",
            f"  - Loss-making: {overall['loss_making_projects_count']}",
            f"  - Break-even: {overall['break_even_projects_count']}",
            "",
            "=" * 80,
            "PROJECT PERFORMANCE ANALYSIS",
            "=" * 80,
            ""
        ])
        
        # Top performing projects
        report_lines.append("TOP 10 PROFITABLE PROJECTS:")
        report_lines.append("-" * 40)
        top_projects = self.financial_summary.nlargest(10, 'Profit')
        for _, project in top_projects.iterrows():
            report_lines.append(
                f"{project['ProjectCode']}: ¥{project['Profit']:,.2f} "
                f"(Margin: {project['ProfitMargin']:.1f}%, "
                f"Hours: {project['TotalHours']:,.0f})"
            )
        
        # Loss-making projects
        report_lines.extend(["", "SIGNIFICANT LOSS-MAKING PROJECTS:", "-" * 40])
        loss_projects = self.analyses['profitability']['loss_making_projects'].head(10)
        if not loss_projects.empty:
            for _, project in loss_projects.iterrows():
                report_lines.append(
                    f"{project['ProjectCode']}: ¥{project['Profit']:,.2f} "
                    f"(Margin: {project['ProfitMargin']:.1f}%)"
                )
        else:
            report_lines.append("No significant loss-making projects found.")
        
        # Efficiency analysis
        report_lines.extend([
            "",
            "=" * 80,
            "EFFICIENCY ANALYSIS",
            "=" * 80,
            ""
        ])
        
        eff_dist = self.analyses['efficiency']['efficiency_distribution']
        report_lines.extend([
            f"Average Efficiency Score: {eff_dist['mean']:.2f}",
            f"Median Efficiency Score: {eff_dist['median']:.2f}",
            f"Standard Deviation: {eff_dist['std']:.2f}",
            "",
            "Efficiency Percentiles:",
            f"  25th: {eff_dist['percentiles']['25%']:.2f}",
            f"  50th: {eff_dist['percentiles']['50%']:.2f}",
            f"  75th: {eff_dist['percentiles']['75%']:.2f}",
            f"  90th: {eff_dist['percentiles']['90%']:.2f}",
            ""
        ])
        
        # Department analysis
        report_lines.extend([
            "=" * 80,
            "DEPARTMENT ANALYSIS",
            "=" * 80,
            ""
        ])
        
        dept_sorted = self.department_summary.sort_values('TotalLaborCost', ascending=False)
        for dept, row in dept_sorted.iterrows():
            report_lines.extend([
                f"{dept}:",
                f"  Total Hours: {row['TotalHours']:,.0f}",
                f"  Total Labor Cost: ¥{row['TotalLaborCost']:,.2f}",
                f"  Projects Involved: {row['NumProjects']}",
                f"  Average Hours per Project: {row['HoursPerProject']:.1f}",
                f"  Hourly Rate: ¥{row['HourlyRate']:.2f}",
                ""
            ])
        
        # Project type comparison
        report_lines.extend([
            "=" * 80,
            "PROJECT TYPE COMPARISON",
            "=" * 80,
            ""
        ])
        
        type_summary = self.financial_summary.groupby('ProjectType').agg({
            'ProjectCode': 'count',
            'ContractPrice': 'sum',
            'Profit': 'sum',
            'ProfitMargin': 'mean',
            'EfficiencyScore': 'mean'
        }).rename(columns={'ProjectCode': 'Count'})
        
        for ptype, row in type_summary.iterrows():
            report_lines.extend([
                f"{ptype} Projects:",
                f"  Count: {row['Count']}",
                f"  Total Revenue: ¥{row['ContractPrice']:,.2f}",
                f"  Total Profit: ¥{row['Profit']:,.2f}",
                f"  Average Profit Margin: {row['ProfitMargin']:.2f}%",
                f"  Average Efficiency: {row['EfficiencyScore']:.2f}",
                ""
            ])
        
        # Status analysis (GS projects only)
        gs_projects = self.financial_summary[self.financial_summary['ProjectType'] == 'GS']
        if not gs_projects.empty:
            report_lines.extend([
                "=" * 80,
                "GS PROJECT STATUS ANALYSIS",
                "=" * 80,
                ""
            ])
            
            status_summary = gs_projects.groupby('Status').agg({
                'ProjectCode': 'count',
                'Profit': ['sum', 'mean'],
                'ProfitMargin': 'mean'
            })
            
            for status in status_summary.index:
                count = status_summary.loc[status, ('ProjectCode', 'count')]
                total_profit = status_summary.loc[status, ('Profit', 'sum')]
                avg_profit = status_summary.loc[status, ('Profit', 'mean')]
                avg_margin = status_summary.loc[status, ('ProfitMargin', 'mean')]
                
                report_lines.extend([
                    f"{status}:",
                    f"  Projects: {count}",
                    f"  Total Profit: ¥{total_profit:,.2f}",
                    f"  Average Profit: ¥{avg_profit:,.2f}",
                    f"  Average Margin: {avg_margin:.2f}%",
                    ""
                ])
        
        # Limitations
        report_lines.extend([
            "=" * 80,
            "LIMITATIONS AND ASSUMPTIONS",
            "=" * 80,
            "",
            "1. Data Quality:",
            "   - Some project codes in work hours may not match project records",
            "   - ISS projects lack status information",
            "   - Some projects may have incomplete data",
            "",
            "2. Financial Assumptions:",
            "   - Department salaries are synthetic estimates",
            "   - Hourly rates calculated based on 2080 work hours/year",
            "   - Overhead costs and indirect expenses not included",
            "   - Exchange rates and inflation not considered",
            "",
            "3. Temporal Constraints:",
            "   - Analysis covers Q3 20X2 only",
            "   - Some projects may be ongoing with incomplete costs",
            "   - Seasonal variations not captured",
            "",
            "=" * 80
        ])
        
        # Save report
        output_path = os.path.join(self.output_dir, 'q3_financial_report.txt')
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report_lines))
        
        logger.info(f"Text report saved to {output_path}")
        
    def generate_executive_summary(self):
        """Generate executive summary"""
        summary = {
            'generated_at': datetime.now().isoformat(),
            'overview': self.analyses['profitability']['overall_metrics'],
            'key_findings': {
                'top_performer': self._get_top_performer(),
                'biggest_concern': self._get_biggest_concern(),
                'most_efficient_department': self._get_most_efficient_department(),
                'project_success_rate': self._calculate_success_rate()
            },
            'financial_highlights': {
                'total_work_hours': float(self.financial_summary['TotalHours'].sum()),
                'average_project_size': float(self.financial_summary['ContractPrice'].mean()),
                'total_labor_cost': float(self.department_summary['TotalLaborCost'].sum()),
                'labor_cost_percentage': self._calculate_labor_percentage()
            },
            'recommendations': [
                "Focus resources on high-efficiency projects",
                "Review and optimize loss-making projects",
                "Improve project status tracking for ISS projects",
                "Standardize project code formats across departments",
                "Implement real-time cost monitoring"
            ]
        }
        
        # Save as JSON
        output_path = os.path.join(self.output_dir, 'executive_summary.json')
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Executive summary saved to {output_path}")
    
    def generate_department_report(self):
        """Generate detailed department report"""
        report_lines = []
        
        report_lines.extend([
            "=" * 80,
            "DEPARTMENT PERFORMANCE REPORT",
            "=" * 80,
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
        ])
        
        # Department rankings
        report_lines.extend([
            "DEPARTMENT RANKINGS",
            "=" * 40,
            "",
            "By Total Hours Worked:",
            "-" * 30
        ])
        
        hours_ranking = self.department_summary.sort_values('TotalHours', ascending=False)
        for i, (dept, row) in enumerate(hours_ranking.iterrows(), 1):
            report_lines.append(f"{i}. {dept}: {row['TotalHours']:,.0f} hours")
        
        report_lines.extend([
            "",
            "By Number of Projects:",
            "-" * 30
        ])
        
        project_ranking = self.department_summary.sort_values('NumProjects', ascending=False)
        for i, (dept, row) in enumerate(project_ranking.iterrows(), 1):
            report_lines.append(f"{i}. {dept}: {row['NumProjects']} projects")
        
        report_lines.extend([
            "",
            "By Labor Cost Efficiency (Cost per Hour):",
            "-" * 30
        ])
        
        cost_efficiency = (self.department_summary['TotalLaborCost'] / 
                          self.department_summary['TotalHours']).sort_values()
        for i, (dept, efficiency) in enumerate(cost_efficiency.items(), 1):
            report_lines.append(f"{i}. {dept}: ¥{efficiency:.2f}/hour")
        
        # Department collaboration matrix
        report_lines.extend([
            "",
            "=" * 80,
            "DEPARTMENT COLLABORATION ANALYSIS",
            "=" * 80,
            ""
        ])
        
        # This would require access to work_hours data for full implementation
        report_lines.append("(Department collaboration matrix requires detailed analysis)")
        
        # Save report
        output_path = os.path.join(self.output_dir, 'department_report.txt')
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report_lines))
        
        logger.info(f"Department report saved to {output_path}")
    
    def generate_status_report(self):
        """Generate project status report for GS projects"""
        gs_projects = self.financial_summary[self.financial_summary['ProjectType'] == 'GS']
        
        if gs_projects.empty:
            logger.warning("No GS projects found for status report")
            return
        
        report_lines = []
        report_lines.extend([
            "=" * 80,
            "GS PROJECT STATUS REPORT",
            "=" * 80,
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
        ])
        
        # Status breakdown
        for status in ['Success', 'Negotiation', 'In Progress', 'Fail', 'Unknown']:
            status_projects = gs_projects[gs_projects['Status'] == status]
            
            if not status_projects.empty:
                report_lines.extend([
                    f"{status.upper()} PROJECTS ({len(status_projects)} projects)",
                    "=" * 40,
                    ""
                ])
                
                # Summary metrics
                report_lines.extend([
                    f"Total Revenue: ¥{status_projects['ContractPrice'].sum():,.2f}",
                    f"Total Profit: ¥{status_projects['Profit'].sum():,.2f}",
                    f"Average Profit Margin: {status_projects['ProfitMargin'].mean():.2f}%",
                    f"Total Hours: {status_projects['TotalHours'].sum():,.0f}",
                    ""
                ])
                
                # List projects
                report_lines.append("Projects:")
                for _, project in status_projects.iterrows():
                    report_lines.append(
                        f"  - {project['ProjectCode']}: {project['ProjectName'][:50]}..."
                        if len(project['ProjectName']) > 50 else 
                        f"  - {project['ProjectCode']}: {project['ProjectName']}"
                    )
                report_lines.append("")
        
        # Save report
        output_path = os.path.join(self.output_dir, 'gs_status_report.txt')
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report_lines))
        
        logger.info(f"Status report saved to {output_path}")
    
    def generate_recommendations(self):
        """Generate recommendations based on analysis"""
        recommendations = []
        
        recommendations.extend([
            "=" * 80,
            "STRATEGIC RECOMMENDATIONS",
            "=" * 80,
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "Based on Q3 20X2 financial analysis, the following recommendations are proposed:",
            ""
        ])
        
        # 1. Project Management Recommendations
        recommendations.extend([
            "1. PROJECT MANAGEMENT",
            "-" * 40,
        ])
        
        # Check for loss-making projects
        loss_count = len(self.financial_summary[self.financial_summary['Profit'] < 0])
        if loss_count > 0:
            recommendations.append(
                f"   • Review {loss_count} loss-making projects for cost optimization"
            )
        
        # Check efficiency variance
        eff_std = self.analyses['efficiency']['efficiency_distribution']['std']
        if eff_std > 100:
            recommendations.append(
                "   • Standardize project management practices to reduce efficiency variance"
            )
        
        # Status-based recommendations
        gs_projects = self.financial_summary[self.financial_summary['ProjectType'] == 'GS']
        if not gs_projects.empty:
            fail_rate = len(gs_projects[gs_projects['Status'] == 'Fail']) / len(gs_projects) * 100
            if fail_rate > 10:
                recommendations.append(
                    f"   • High failure rate ({fail_rate:.1f}%) requires root cause analysis"
                )
        
        recommendations.append("")
        
        # 2. Resource Allocation
        recommendations.extend([
            "2. RESOURCE ALLOCATION",
            "-" * 40,
        ])
        
        # Department efficiency
        dept_costs = self.department_summary['TotalLaborCost'].sort_values(ascending=False)
        top_cost_dept = dept_costs.index[0]
        recommendations.append(
            f"   • {top_cost_dept} has highest labor cost - evaluate resource utilization"
        )
        
        # Project distribution
        avg_projects_per_dept = self.department_summary['NumProjects'].mean()
        overloaded_depts = self.department_summary[
            self.department_summary['NumProjects'] > avg_projects_per_dept * 1.5
        ]
        if not overloaded_depts.empty:
            recommendations.append(
                f"   • Balance workload for {len(overloaded_depts)} overloaded departments"
            )
        
        recommendations.append("")
        
        # 3. Financial Optimization
        recommendations.extend([
            "3. FINANCIAL OPTIMIZATION",
            "-" * 40,
        ])
        
        # Profit margin analysis
        avg_margin = self.analyses['profitability']['overall_metrics']['average_profit_margin']
        if avg_margin < 15:
            recommendations.append(
                f"   • Average profit margin ({avg_margin:.1f}%) below industry standard"
            )
            recommendations.append(
                "   • Consider pricing strategy review or cost reduction initiatives"
            )
        
        # Labor cost percentage
        labor_percentage = self._calculate_labor_percentage()
        if labor_percentage > 40:
            recommendations.append(
                f"   • High labor cost percentage ({labor_percentage:.1f}%) - explore automation"
            )
        
        recommendations.append("")
        
        # 4. Data Quality
        recommendations.extend([
            "4. DATA QUALITY IMPROVEMENTS",
            "-" * 40,
            "   • Standardize project code format across all departments",
            "   • Implement status tracking for ISS projects",
            "   • Ensure all work hours are properly coded to projects",
            "   • Regular data validation to prevent missing information",
            ""
        ])
        
        # 5. Strategic Initiatives
        recommendations.extend([
            "5. STRATEGIC INITIATIVES",
            "-" * 40,
        ])
        
        # High performers
        top_performers = self.analyses['efficiency']['top_efficient_projects']
        if not top_performers.empty:
            recommendations.append(
                "   • Analyze success factors of top-performing projects for replication"
            )
        
        # Project type comparison
        type_profits = self.financial_summary.groupby('ProjectType')['Profit'].sum()
        if len(type_profits) > 1:
            best_type = type_profits.idxmax()
            recommendations.append(
                f"   • {best_type} projects show higher profitability - consider focus shift"
            )
        
        recommendations.extend([
            "",
            "=" * 80,
            "IMPLEMENTATION PRIORITY",
            "=" * 80,
            "",
            "HIGH PRIORITY (Immediate action required):",
            "  1. Address loss-making projects",
            "  2. Implement data quality improvements",
            "  3. Review failed project root causes",
            "",
            "MEDIUM PRIORITY (Within next quarter):",
            "  1. Optimize resource allocation",
            "  2. Standardize project management practices",
            "  3. Develop efficiency benchmarks",
            "",
            "LOW PRIORITY (Long-term initiatives):",
            "  1. Automation exploration",
            "  2. Strategic portfolio rebalancing",
            "  3. Advanced analytics implementation",
            ""
        ])
        
        # Save recommendations
        output_path = os.path.join(self.output_dir, 'strategic_recommendations.txt')
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(recommendations))
        
        logger.info(f"Recommendations saved to {output_path}")
    
    def _get_top_performer(self) -> Dict:
        """Get top performing project details"""
        if self.financial_summary.empty:
            return {}
        
        top = self.financial_summary.nlargest(1, 'Profit').iloc[0]
        return {
            'project_code': top['ProjectCode'],
            'project_name': top['ProjectName'],
            'profit': float(top['Profit']),
            'margin': float(top['ProfitMargin'])
        }
    
    def _get_biggest_concern(self) -> Dict:
        """Get biggest concern project"""
        if self.financial_summary.empty:
            return {}
        
        concern = self.financial_summary.nsmallest(1, 'Profit').iloc[0]
        return {
            'project_code': concern['ProjectCode'],
            'project_name': concern['ProjectName'],
            'loss': float(concern['Profit']),
            'margin': float(concern['ProfitMargin'])
        }
    
    def _get_most_efficient_department(self) -> str:
        """Get most efficient department"""
        if self.department_summary.empty:
            return "Unknown"
        
        efficiency = (self.department_summary['NumProjects'] / 
                     self.department_summary['TotalHours'] * 1000)
        return efficiency.idxmax()
    
    def _calculate_success_rate(self) -> float:
        """Calculate project success rate for GS projects"""
        gs_projects = self.financial_summary[self.financial_summary['ProjectType'] == 'GS']
        if gs_projects.empty:
            return 0.0
        
        success_count = len(gs_projects[gs_projects['Status'] == 'Success'])
        total_count = len(gs_projects[gs_projects['Status'] != 'Unknown'])
        
        return (success_count / total_count * 100) if total_count > 0 else 0.0
    
    def _calculate_labor_percentage(self) -> float:
        """Calculate labor cost as percentage of total cost"""
        total_labor = self.department_summary['TotalLaborCost'].sum()
        total_cost = self.financial_summary['TotalCost'].sum()
        
        return (total_labor / total_cost * 100) if total_cost > 0 else 0.0


def main():
    """Test report generation"""
    from data_loader import DataLoader
    from data_processor import DataProcessor
    
    # Load and process data
    loader = DataLoader()
    work_hours, gs_projects, iss_projects = loader.load_all_data()
    combined_projects = loader.combine_projects()
    
    processor = DataProcessor(work_hours, combined_projects)
    financial_summary = processor.calculate_all_metrics()
    
    # Get analyses
    analyses = {
        'efficiency': processor.get_efficiency_analysis(),
        'profitability': processor.get_profitability_analysis()
    }
    
    # Generate reports
    generator = ReportGenerator(
        financial_summary,
        processor.department_summary_df,
        analyses
    )
    generator.generate_all_reports()


if __name__ == "__main__":
    main()