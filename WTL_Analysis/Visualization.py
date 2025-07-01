import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import matplotlib.pyplot as plt
import seaborn as sns
import os
from typing import Dict, List
import logging
from config import REPORT_CONFIG, COLOR_STATUS_MAP

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Visualizer:
    """Create visualizations for financial analysis"""
    
    def __init__(self, financial_summary_df: pd.DataFrame, 
                 department_summary_df: pd.DataFrame,
                 work_hours_df: pd.DataFrame):
        self.financial_summary = financial_summary_df
        self.department_summary = department_summary_df
        self.work_hours = work_hours_df
        
        # Create output directories
        self.output_dir = REPORT_CONFIG['visualizations_dir']
        os.makedirs(self.output_dir, exist_ok=True)
        
    def create_all_visualizations(self):
        """Generate all visualizations"""
        logger.info("Creating visualizations...")
        
        # Main dashboard
        self.create_main_dashboard()
        
        # Individual visualizations
        self.create_profitability_charts()
        self.create_efficiency_analysis()
        self.create_department_analysis()
        self.create_project_status_analysis()
        self.create_time_series_analysis()
        
        logger.info(f"All visualizations saved to {self.output_dir}")
    
    def create_main_dashboard(self):
        """Create main financial dashboard"""
        fig = make_subplots(
            rows=3, cols=2,
            subplot_titles=(
                'Top 10 Projects by Profit',
                'Project Status Distribution',
                'Department Work Hours',
                'Profit Margin Distribution',
                'Revenue vs Cost Analysis',
                'Efficiency Heatmap'
            ),
            specs=[
                [{"type": "bar"}, {"type": "pie"}],
                [{"type": "pie"}, {"type": "box"}],
                [{"type": "scatter"}, {"type": "bar"}]
            ],
            row_heights=[0.33, 0.33, 0.34],
            vertical_spacing=0.1,
            horizontal_spacing=0.1
        )
        
        # 1. Top 10 Projects by Profit
        top_projects = self.financial_summary.nlargest(10, 'Profit')
        fig.add_trace(
            go.Bar(
                x=top_projects['ProjectCode'],
                y=top_projects['Profit'],
                text=top_projects['Profit'].round(0).astype(str),
                textposition='outside',
                marker_color='lightgreen',
                name='Profit'
            ),
            row=1, col=1
        )
        
        # 2. Project Status Distribution (GS projects only)
        gs_projects = self.financial_summary[self.financial_summary['ProjectType'] == 'GS']
        status_counts = gs_projects['Status'].value_counts()
        
        colors = {
            'Success': 'green',
            'Negotiation': 'darkgreen',
            'In Progress': 'lightgray',
            'Fail': 'red',
            'Unknown': 'blue'
        }
        
        fig.add_trace(
            go.Pie(
                labels=status_counts.index,
                values=status_counts.values,
                marker=dict(colors=[colors.get(s, 'gray') for s in status_counts.index]),
                textinfo='label+percent',
                name='Status'
            ),
            row=1, col=2
        )
        
        # 3. Department Work Hours
        fig.add_trace(
            go.Pie(
                labels=self.department_summary.index,
                values=self.department_summary['TotalHours'],
                textinfo='label+percent',
                hole=0.4,
                name='Hours'
            ),
            row=2, col=1
        )
        
        # 4. Profit Margin Distribution by Project Type
        for ptype in self.financial_summary['ProjectType'].unique():
            type_data = self.financial_summary[
                (self.financial_summary['ProjectType'] == ptype) &
                (self.financial_summary['ProfitMargin'].notna())
            ]
            fig.add_trace(
                go.Box(
                    y=type_data['ProfitMargin'],
                    name=ptype,
                    boxpoints='outliers'
                ),
                row=2, col=2
            )
        
        # 5. Revenue vs Cost Scatter
        fig.add_trace(
            go.Scatter(
                x=self.financial_summary['TotalCost'],
                y=self.financial_summary['ContractPrice'],
                mode='markers',
                marker=dict(
                    size=8,
                    color=self.financial_summary['ProfitMargin'],
                    colorscale='RdYlGn',
                    showscale=True,
                    colorbar=dict(
                        title="Profit<br>Margin %",
                        x=0.45,
                        y=0.15,
                        len=0.3
                    )
                ),
                text=self.financial_summary['ProjectCode'],
                hovertemplate='%{text}<br>Cost: %{x:,.0f}<br>Revenue: %{y:,.0f}',
                name='Projects'
            ),
            row=3, col=1
        )
        
        # Add break-even line
        max_val = max(self.financial_summary['TotalCost'].max(), 
                     self.financial_summary['ContractPrice'].max())
        fig.add_trace(
            go.Scatter(
                x=[0, max_val],
                y=[0, max_val],
                mode='lines',
                line=dict(color='gray', dash='dash'),
                showlegend=False,
                name='Break-even'
            ),
            row=3, col=1
        )
        
        # 6. Department Efficiency
        dept_efficiency = self.work_hours.groupby('Department').apply(
            lambda x: self._calculate_dept_efficiency(x)
        ).sort_values(ascending=False)
        
        fig.add_trace(
            go.Bar(
                x=dept_efficiency.index,
                y=dept_efficiency.values,
                marker_color='lightcoral',
                name='Efficiency'
            ),
            row=3, col=2
        )
        
        # Update layout
        fig.update_layout(
            title_text="WTL Q3 Financial Dashboard",
            showlegend=False,
            height=1200,
            title_font_size=20
        )
        
        # Update axes
        fig.update_xaxes(title_text="Project Code", row=1, col=1)
        fig.update_yaxes(title_text="Profit (¥)", row=1, col=1)
        
        fig.update_xaxes(title_text="Project Type", row=2, col=2)
        fig.update_yaxes(title_text="Profit Margin (%)", row=2, col=2)
        
        fig.update_xaxes(title_text="Total Cost (¥)", row=3, col=1)
        fig.update_yaxes(title_text="Contract Price (¥)", row=3, col=1)
        
        fig.update_xaxes(title_text="Department", row=3, col=2, tickangle=45)
        fig.update_yaxes(title_text="Avg Efficiency Score", row=3, col=2)
        
        # Save
        output_path = os.path.join(self.output_dir, 'main_dashboard.html')
        fig.write_html(output_path)
        logger.info(f"Main dashboard saved to {output_path}")
    
    def _calculate_dept_efficiency(self, dept_data: pd.DataFrame) -> float:
        """Calculate average efficiency for department projects"""
        projects = dept_data['ProjectCode'].unique()
        efficiencies = []
        
        for project in projects:
            if project in self.financial_summary['ProjectCode'].values:
                eff = self.financial_summary[
                    self.financial_summary['ProjectCode'] == project
                ]['EfficiencyScore'].values
                if len(eff) > 0:
                    efficiencies.append(eff[0])
        
        return np.mean(efficiencies) if efficiencies else 0
    
    def create_profitability_charts(self):
        """Create profitability analysis charts"""
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=(
                'Profit Distribution',
                'Profit by Project Type',
                'Top Loss-Making Projects',
                'Profit Margin Trends'
            )
        )
        
        # 1. Profit Distribution Histogram
        fig.add_trace(
            go.Histogram(
                x=self.financial_summary['Profit'],
                nbinsx=30,
                name='Profit Distribution',
                marker_color='lightblue'
            ),
            row=1, col=1
        )
        
        # 2. Profit by Project Type
        profit_by_type = self.financial_summary.groupby('ProjectType')['Profit'].sum()
        fig.add_trace(
            go.Bar(
                x=profit_by_type.index,
                y=profit_by_type.values,
                text=profit_by_type.values.round(0).astype(str),
                textposition='outside',
                marker_color=['lightgreen', 'lightcoral']
            ),
            row=1, col=2
        )
        
        # 3. Top Loss-Making Projects
        loss_projects = self.financial_summary[
            self.financial_summary['Profit'] < 0
        ].nsmallest(10, 'Profit')
        
        if not loss_projects.empty:
            fig.add_trace(
                go.Bar(
                    x=loss_projects['Profit'],
                    y=loss_projects['ProjectCode'],
                    orientation='h',
                    text=loss_projects['Profit'].round(0).astype(str),
                    textposition='outside',
                    marker_color='red'
                ),
                row=2, col=1
            )
        
        # 4. Profit Margin by Status
        margin_by_status = self.financial_summary[
            self.financial_summary['Status'] != 'Unknown'
        ].groupby('Status')['ProfitMargin'].mean().sort_values(ascending=False)
        
        fig.add_trace(
            go.Bar(
                x=margin_by_status.index,
                y=margin_by_status.values,
                text=margin_by_status.values.round(1).astype(str) + '%',
                textposition='outside',
                marker_color='lightyellow'
            ),
            row=2, col=2
        )
        
        # Update layout
        fig.update_layout(
            title_text="Profitability Analysis",
            showlegend=False,
            height=800
        )
        
        # Save
        output_path = os.path.join(self.output_dir, 'profitability_analysis.html')
        fig.write_html(output_path)
        logger.info(f"Profitability analysis saved to {output_path}")
    
    def create_efficiency_analysis(self):
        """Create efficiency analysis visualizations"""
        # Create figure
        fig = go.Figure()
        
        # Efficiency vs Hours scatter plot
        fig.add_trace(
            go.Scatter(
                x=self.financial_summary['TotalHours'],
                y=self.financial_summary['EfficiencyScore'],
                mode='markers',
                marker=dict(
                    size=self.financial_summary['ContractPrice'] / 100000,
                    color=self.financial_summary['ProfitMargin'],
                    colorscale='RdYlGn',
                    showscale=True,
                    colorbar=dict(title="Profit Margin %"),
                    sizemode='area',
                    sizeref=2.*max(self.financial_summary['ContractPrice'] / 100000)/(40.**2),
                    sizemin=4
                ),
                text=self.financial_summary['ProjectCode'],
                hovertemplate=(
                    '%{text}<br>' +
                    'Hours: %{x:,.0f}<br>' +
                    'Efficiency: %{y:,.0f}<br>' +
                    'Size: Contract Price'
                )
            )
        )
        
        # Add trend line
        from scipy import stats
        mask = (self.financial_summary['TotalHours'] > 0) & \
               (self.financial_summary['EfficiencyScore'].notna())
        x = self.financial_summary.loc[mask, 'TotalHours']
        y = self.financial_summary.loc[mask, 'EfficiencyScore']
        
        if len(x) > 2:
            slope, intercept, _, _, _ = stats.linregress(x, y)
            x_trend = np.array([x.min(), x.max()])
            y_trend = slope * x_trend + intercept
            
            fig.add_trace(
                go.Scatter(
                    x=x_trend,
                    y=y_trend,
                    mode='lines',
                    line=dict(color='red', dash='dash'),
                    name='Trend Line'
                )
            )
        
        fig.update_layout(
            title="Project Efficiency Analysis",
            xaxis_title="Total Hours",
            yaxis_title="Efficiency Score (Profit/Hour)",
            height=600
        )
        
        # Save
        output_path = os.path.join(self.output_dir, 'efficiency_analysis.html')
        fig.write_html(output_path)
        logger.info(f"Efficiency analysis saved to {output_path}")
    
    def create_department_analysis(self):
        """Create department analysis visualizations"""
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=(
                'Department Labor Costs',
                'Hours per Project by Department',
                'Department Productivity',
                'Cost Efficiency by Department'
            )
        )
        
        # 1. Department Labor Costs
        fig.add_trace(
            go.Bar(
                x=self.department_summary.index,
                y=self.department_summary['TotalLaborCost'],
                text=self.department_summary['TotalLaborCost'].round(0).astype(str),
                textposition='outside',
                marker_color='lightcoral'
            ),
            row=1, col=1
        )
        
        # 2. Hours per Project
        fig.add_trace(
            go.Bar(
                x=self.department_summary.index,
                y=self.department_summary['HoursPerProject'],
                text=self.department_summary['HoursPerProject'].round(1).astype(str),
                textposition='outside',
                marker_color='lightblue'
            ),
            row=1, col=2
        )
        
        # 3. Department Productivity (Projects per 1000 hours)
        productivity = (self.department_summary['NumProjects'] / 
                       self.department_summary['TotalHours'] * 1000)
        
        fig.add_trace(
            go.Scatter(
                x=self.department_summary.index,
                y=productivity,
                mode='markers+lines',
                marker=dict(size=10, color='green'),
                line=dict(color='green')
            ),
            row=2, col=1
        )
        
        # 4. Cost Efficiency
        cost_per_project = (self.department_summary['TotalLaborCost'] / 
                           self.department_summary['NumProjects'])
        
        fig.add_trace(
            go.Bar(
                x=self.department_summary.index,
                y=cost_per_project,
                text=cost_per_project.round(0).astype(str),
                textposition='outside',
                marker_color='lightyellow'
            ),
            row=2, col=2
        )
        
        # Update layout
        fig.update_layout(
            title_text="Department Analysis",
            showlegend=False,
            height=800
        )
        
        # Update axes
        fig.update_xaxes(tickangle=45)
        
        # Save
        output_path = os.path.join(self.output_dir, 'department_analysis.html')
        fig.write_html(output_path)
        logger.info(f"Department analysis saved to {output_path}")
    
    def create_project_status_analysis(self):
        """Create project status analysis for GS projects"""
        gs_projects = self.financial_summary[self.financial_summary['ProjectType'] == 'GS']
        
        if gs_projects.empty:
            logger.warning("No GS projects found for status analysis")
            return
        
        # Status metrics
        status_metrics = gs_projects.groupby('Status').agg({
            'ProjectCode': 'count',
            'ContractPrice': 'sum',
            'Profit': 'sum',
            'ProfitMargin': 'mean',
            'TotalHours': 'sum'
        }).rename(columns={'ProjectCode': 'Count'})
        
        # Create subplots
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=(
                'Projects by Status',
                'Revenue by Status',
                'Average Profit Margin by Status',
                'Total Hours by Status'
            ),
            specs=[[{"type": "pie"}, {"type": "pie"}],
                   [{"type": "bar"}, {"type": "bar"}]]
        )
        
        # Define colors
        status_colors = {
            'Success': 'green',
            'Negotiation': 'darkgreen',
            'In Progress': 'lightgray',
            'Fail': 'red',
            'Unknown': 'blue'
        }
        
        colors = [status_colors.get(s, 'gray') for s in status_metrics.index]
        
        # 1. Project count by status
        fig.add_trace(
            go.Pie(
                labels=status_metrics.index,
                values=status_metrics['Count'],
                marker=dict(colors=colors),
                textinfo='label+value'
            ),
            row=1, col=1
        )
        
        # 2. Revenue by status
        fig.add_trace(
            go.Pie(
                labels=status_metrics.index,
                values=status_metrics['ContractPrice'],
                marker=dict(colors=colors),
                textinfo='label+percent'
            ),
            row=1, col=2
        )
        
        # 3. Average profit margin by status
        fig.add_trace(
            go.Bar(
                x=status_metrics.index,
                y=status_metrics['ProfitMargin'],
                text=status_metrics['ProfitMargin'].round(1).astype(str) + '%',
                textposition='outside',
                marker_color=colors
            ),
            row=2, col=1
        )
        
        # 4. Total hours by status
        fig.add_trace(
            go.Bar(
                x=status_metrics.index,
                y=status_metrics['TotalHours'],
                text=status_metrics['TotalHours'].round(0).astype(str),
                textposition='outside',
                marker_color=colors
            ),
            row=2, col=2
        )
        
        # Update layout
        fig.update_layout(
            title_text="GS Project Status Analysis",
            showlegend=False,
            height=800
        )
        
        # Save
        output_path = os.path.join(self.output_dir, 'project_status_analysis.html')
        fig.write_html(output_path)
        logger.info(f"Project status analysis saved to {output_path}")
    
    def create_time_series_analysis(self):
        """Create time series analysis of work patterns"""
        # Extract week numbers from date ranges
        self.work_hours['Week'] = self.work_hours['Date'].str.extract(r'(\d+/\d+)')
        
        # Aggregate by week
        weekly_hours = self.work_hours.groupby('Week')['Hours'].sum().sort_index()
        weekly_projects = self.work_hours.groupby('Week')['ProjectCode'].nunique()
        
        # Create figure
        fig = make_subplots(
            rows=2, cols=1,
            subplot_titles=('Weekly Work Hours', 'Active Projects per Week'),
            shared_xaxes=True
        )
        
        # 1. Weekly hours
        fig.add_trace(
            go.Scatter(
                x=weekly_hours.index,
                y=weekly_hours.values,
                mode='lines+markers',
                line=dict(color='blue', width=2),
                marker=dict(size=8),
                name='Total Hours'
            ),
            row=1, col=1
        )
        
        # 2. Active projects
        fig.add_trace(
            go.Bar(
                x=weekly_projects.index,
                y=weekly_projects.values,
                marker_color='lightgreen',
                name='Active Projects'
            ),
            row=2, col=1
        )
        
        # Update layout
        fig.update_layout(
            title_text="Time Series Analysis - Q3 Work Patterns",
            showlegend=False,
            height=600
        )
        
        fig.update_xaxes(title_text="Week", row=2, col=1)
        fig.update_yaxes(title_text="Total Hours", row=1, col=1)
        fig.update_yaxes(title_text="Number of Projects", row=2, col=1)
        
        # Save
        output_path = os.path.join(self.output_dir, 'time_series_analysis.html')
        fig.write_html(output_path)
        logger.info(f"Time series analysis saved to {output_path}")


def main():
    """Test visualization creation"""
    from data_loader import DataLoader
    from data_processor import DataProcessor
    
    # Load and process data
    loader = DataLoader()
    work_hours, gs_projects, iss_projects = loader.load_all_data()
    combined_projects = loader.combine_projects()
    
    processor = DataProcessor(work_hours, combined_projects)
    financial_summary = processor.calculate_all_metrics()
    
    # Create visualizations
    visualizer = Visualizer(
        financial_summary,
        processor.department_summary_df,
        work_hours
    )
    visualizer.create_all_visualizations()


if __name__ == "__main__":
    main()