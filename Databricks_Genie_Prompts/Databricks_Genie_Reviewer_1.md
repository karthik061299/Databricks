_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*:   Comprehensive review and refinement of Databricks Genie prompts for insightful, user-friendly dashboards
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# Databricks Genie Reviewer

## Table of Contents
1. Metadata Requirements
2. Reviewer Test Case Structure
3. Input Data Sources & Transformations
4. Visual Prompt Review
5. Visual Selection & Recommendations
6. Dashboard Expansion Suggestions
7. Data Field & Calculation Validation
8. Formatting & Best Practices
9. Usability Enhancements
10. Final Recommendations

---

## 1. Metadata Requirements
- Author: AAVA
- Created on: *(auto-filled by system)*
- Description: Comprehensive review and refinement of Databricks Genie prompts for insightful, user-friendly dashboards
- Version: 1
- Updated on: *(auto-filled by system)*

---

## 2. Reviewer Test Case Structure
This document reviews Databricks Genie prompts to ensure they:
- Start with "Create" for the first visual and "Add" for subsequent visuals
- Are complete, clear, and aligned with reporting requirements
- Use appropriate visual types for the reporting need
- Suggest alternatives if better visuals exist
- Expand the dashboard with at least 2–3 additional visuals if needed
- Confirm necessary fields, aggregations, calculations, and filters are specified
- Specify titles, axis labels, colors, and legends
- Maintain consistent formatting
- Suggest interactivity features (filters, parameters, drill-throughs)
- Recommend narratives or KPI comparisons
- Follow Databricks visualization best practices
- Maintain consistency in structure, terminology, and formatting

---

## 3. Input Data Sources & Transformations
**Data Sources:**
- Visualddata: `available_visuals.txt` ([Input Repo](https://github.com/karthik061299/Databricks/tree/main/Input))
- Inventory: `Inventory_Management_Reports.txt` ([Input Repo](https://github.com/karthik061299/Databricks/tree/main/Input))
- Bronze Physical Model: `Databricks_Gold_Model_Physical_1.txt` ([Gold Layer Repo](https://github.com/karthik061299/Databricks/tree/main/Databricks_Data_Modeler_Gold_Layer))

**Transformations:**
- Joins between sales, product, and inventory tables
- Aggregations (SUM, AVG, COUNT, etc.)
- Filters (date ranges, categories, regions)
- Output formats: dashboards, charts, tables

---

## 4. Visual Prompt Review
**Prompt Structure:**
- First visual prompt begins with "Create"
- Subsequent prompts begin with "Add"

**Sample Prompts:**
- Create a line chart showing monthly revenue trends for the last 12 months from `sales_fact` table
- Add a bar chart comparing total inventory levels by product category from `inventory_fact` table
- Add a KPI card for average stock level with conditional formatting

---

## 5. Visual Selection & Recommendations
| Visual Type      | Use Case                                 | Reviewer Recommendation                      |
|------------------|------------------------------------------|----------------------------------------------|
| Line Chart       | Time series (revenue, trends)            | Recommended for trends over time             |
| Bar Chart        | Category comparison                      | Use for product/category comparisons         |
| KPI Card         | Key metrics (totals, averages)           | Use for high-level KPIs                      |
| Map              | Geospatial analysis                      | Use for region/store performance             |
| Treemap          | Hierarchical category breakdown          | Use for market share or segment analysis     |
| Heatmap          | Distribution, density, data quality      | Use for anomaly or completeness checks       |
| Funnel           | Conversion/attrition analysis            | Use for customer journey or escalation rates |

---

## 6. Dashboard Expansion Suggestions
- Add a trend analysis visual (e.g., line chart for sales over time)
- Add a comparison visual (e.g., bar chart for revenue by region)
- Add a breakdown visual (e.g., treemap for product categories)
- Add KPI cards for key metrics (e.g., total revenue, average inventory)
- Add a heatmap for data quality or anomaly detection

---

## 7. Data Field & Calculation Validation
- Ensure all prompts specify required fields from the data model
- Confirm aggregations (SUM, AVG, COUNT) are correctly applied
- Validate filters (date, region, category) are present and accurate
- Check for correct use of joins and intermediate transformations

---

## 8. Formatting & Best Practices
- All visuals should specify titles, axis labels, colors, and legends
- Use consistent color schemes (Databricks palette: blue, green, orange)
- Maintain uniform typography (sans-serif fonts)
- Use grid layout: KPIs on top, trends in the middle, details at the bottom
- Document all dashboard components and provide usage examples

---

## 9. Usability Enhancements
- Add interactive filters (date, category, region)
- Enable drill-through from summary to detail
- Suggest dynamic parameters for cohort or metric selection
- Recommend real-time updates for operational metrics
- Provide narratives or KPI comparisons for better storytelling

---

## 10. Final Recommendations
- All prompts follow Databricks visualization best practices
- Consistent structure, terminology, and formatting maintained
- Expanded dashboard with at least 3 additional visuals for richer insights
- Interactivity and usability features recommended
- Data quality and completeness checks included
- Ready for implementation and future versioning

---

**End of Reviewer Output**
