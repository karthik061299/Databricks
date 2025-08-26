_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*:   Refined reviewer for Databricks Genie prompts for Inventory Management, ensuring alignment with reporting requirements, optimal visuals, and dashboard best practices.
## *Version*: 2 
## *Updated on*: 
_____________________________________________

# Databricks Genie Reviewer for Inventory Management

## Overview
This document reviews and refines the Databricks Genie prompts for Inventory Management, ensuring clarity, completeness, and alignment with reporting requirements and Databricks dashboard best practices. It optimizes visual selection, expands dashboard insights, and enhances usability.

---

## 1. Visual Prompt Review & Recommendations

| # | Prompt                                                                                          | Visual Type      | Data Fields                                 | Aggregations/Calculations           | Interactivity Features                  | Rationale & Best Practices                |
|---|-------------------------------------------------------------------------------------------------|------------------|----------------------------------------------|-------------------------------------|------------------------------------------|--------------------------------------------|
| 1 | Create a bar chart to compare inventory turnover rates across product categories.                | Bar Chart        | category, total_stock, total_sold, turnover_rate | SUM, division                      | Filter by category, drill-down to SKU    | Bar chart ideal for categorical comparison. Use clear axis labels, legend, and color differentiation. |
| 2 | Add KPI cards for current stock, reorder point, and safety stock by warehouse.                  | KPI Card         | warehouse_id, current_stock, reorder_point, safety_stock | SUM                                | Filter by warehouse, drill-through       | KPI cards highlight critical metrics. Use color coding (red/yellow/green) for status. |
| 3 | Add a line chart to visualize demand forecast trends by product and region.                      | Line Chart       | product_id, region, avg_demand, accuracy     | AVG                                 | Filter by region, time period            | Line chart for trend analysis. Use time on X-axis, legend for regions. |
| 4 | Add a histogram to show distribution of inventory aging (days in inventory) by product.          | Histogram        | product_id, days_in_inventory                | DATEDIFF                            | Filter by product, drill-down to warehouse| Histogram for distribution analysis. Use bins, axis labels. |
| 5 | Add an area chart to display stockout events over time.                                          | Area Chart       | date_key, stockout_events                    | COUNT                               | Filter by date, drill-down to category   | Area chart for cumulative events. Use time axis, color for event severity. |
| 6 | Add a table for detailed inventory metrics by product and warehouse.                             | Table            | product_id, warehouse_id, current_stock, reorder_point, safety_stock, turnover_rate | SUM, division                      | Sort, filter, drill-through              | Table for granular details. Ensure sortable columns, clear formatting. |

---

## 2. Additional Visuals for Richer Insights

| # | Prompt                                                                                          | Visual Type      | Data Fields                                 | Aggregations/Calculations           | Interactivity Features                  | Rationale & Best Practices                |
|---|-------------------------------------------------------------------------------------------------|------------------|----------------------------------------------|-------------------------------------|------------------------------------------|--------------------------------------------|
| 7 | Add a scatter plot to analyze correlation between forecast accuracy and turnover rate by product.| Scatter Plot     | product_id, accuracy, turnover_rate          | None                                | Filter by product, region                | Scatter plot for correlation analysis. Use tooltips, axis labels. |
| 8 | Add a pie chart to visualize composition of inventory by category (top 5 categories).            | Pie Chart        | category, total_stock                        | SUM                                 | Filter by category                       | Pie chart for part-to-whole analysis. Limit to ≤5 categories for clarity. |
| 9 | Add a tree map to show hierarchical breakdown of inventory by category and sub-category.         | Tree Map         | category, sub_category, total_stock          | SUM                                 | Drill-down to sub-category               | Tree map for nested categories. Use color and size for hierarchy. |

---

## 3. Dashboard Layout & Formatting
- **Top Row**: KPI cards for stock metrics
- **Middle**: Bar chart (turnover), line chart (forecast), area chart (stockouts)
- **Bottom**: Table (details), histogram (aging), scatter plot (correlation)
- **Sidebar**: Filters for category, warehouse, region, time period
- **Color Scheme**: Use organization branding; differentiate visuals with blue, green, orange; KPI status with red/yellow/green
- **Typography**: Sans-serif fonts, bold for KPIs, consistent sizes
- **Titles/Labels**: Each visual must have clear title, axis labels, legend

---

## 4. Data Field & Transformation Validation
- All prompts include necessary fields from the physical model and inventory tables
- Aggregations (SUM, AVG, COUNT, DATEDIFF, division) are specified for each visual
- Filters and drill-throughs are defined for interactivity
- Calculations (turnover rate, aging) are clearly described

---

## 5. Dashboard Usability Enhancements
- **Filters**: Category, warehouse, region, time period
- **Slicers**: Product, supplier
- **Drill-through**: From summary to SKU/warehouse details
- **Drill up/down**: Aggregate by category, drill to SKU
- **Narratives**: Add KPI comparisons and trend commentary for storytelling

---

## 6. Performance & Optimization Best Practices
- Use Delta Lake for all tables
- Partition fact tables by date_key, region
- Cache dimension tables for frequent joins
- Z-order by product_id, warehouse_id for filtering
- Use bucketing for high-cardinality columns
- Monitor query performance and optimize joins

---

## 7. Reviewer Approval Checklist
- [x] First visual prompt starts with "Create", others with "Add"
- [x] All prompts are complete, clear, and aligned with requirements
- [x] Visual types match reporting needs; alternatives suggested
- [x] Dashboard expanded with additional visuals
- [x] Data fields, aggregations, and filters validated
- [x] Formatting and layout best practices applied
- [x] Usability features (filters, drill-throughs, narratives) included
- [x] Consistent structure and terminology maintained

---

## 8. Tips & Pitfalls
- Avoid inefficient joins; cache dimension tables
- Monitor query performance for shuffles/skew
- Use Delta Lake optimization (Z-order, partitioning)
- Keep layout simple and user-friendly
- Regularly review metrics and filters for business relevance

---

## 9. References
- [Databricks Visualization Documentation](https://docs.databricks.com/en/dashboards/index.html)
- [Delta Lake Optimization](https://docs.databricks.com/en/delta/optimizations.html)

---

This reviewer output ensures the Databricks Genie prompts are refined for optimal dashboard creation, actionable insights, and best practices adherence.
