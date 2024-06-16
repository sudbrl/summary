import dask.dataframe as dd
import pandas as pd
import streamlit as st
from openpyxl import load_workbook
from openpyxl.styles import Font
from io import BytesIO

def read_excel_sheets(file):
    sheets = pd.read_excel(file, sheet_name=None)
    dask_sheets = {sheet_name: dd.from_pandas(sheet_df, npartitions=1) for sheet_name, sheet_df in sheets.items()}
    return dask_sheets

def calculate_common_actype_desc(sheets_1, sheets_2):
    result = {}
    for sheet_name in sheets_1:
        if sheet_name in sheets_2:
            df1 = sheets_1[sheet_name]
            df2 = sheets_2[sheet_name]

            # Ensure required columns exist
            if all(col in df1.columns for col in ['Ac Type Desc', 'Balance', 'Main Code', 'Limit']) and \
               all(col in df2.columns for col in ['Ac Type Desc', 'Balance', 'Main Code', 'Limit']):
                
                # Exclude rows with Limit == 0
                df1 = df1[df1['Limit'] != 0]
                df2 = df2[df2['Limit'] != 0]

                # Filter out rows where 'Main Code' is 'AcType Total' or 'Grand Total'
                df1 = df1[~df1['Main Code'].isin(['AcType Total', 'Grand Total'])]
                df2 = df2[~df2['Main Code'].isin(['AcType Total', 'Grand Total'])]

                # Group by 'Ac Type Desc' and calculate sum and count
                df1_grouped = df1.groupby('Ac Type Desc').agg({'Balance': 'sum', 'Ac Type Desc': 'count'}).compute()
                df2_grouped = df2.groupby('Ac Type Desc').agg({'Balance': 'sum', 'Ac Type Desc': 'count'}).compute()
                
                # Rename columns with appropriate names for previous and new sheets
                df1_grouped.columns = ['Previous Balance Sum', 'Previous Count']
                df2_grouped.columns = ['New Balance Sum', 'New Count']
                
                # Merge dataframes on 'Ac Type Desc' with a full outer join
                combined_df = pd.merge(df1_grouped, df2_grouped, left_index=True, right_index=True, how='outer')
                
                # Replace NaN values with 0
                combined_df = combined_df.fillna(0)

                # Add total row
                total_row = pd.DataFrame(combined_df.sum()).transpose()
                total_row.index = ['Total']
                combined_df = pd.concat([combined_df, total_row])
                
                # Select relevant columns for output
                result_df = combined_df.reset_index()
                result[sheet_name] = result_df
    return result

def autofit_excel(writer):
    for sheet_name in writer.sheets:
        worksheet = writer.sheets[sheet_name]
        for column_cells in worksheet.columns:
            max_length = 0
            column = column_cells[0].column_letter
            for cell in column_cells:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = (max_length + 2)
            worksheet.column_dimensions[column].width = adjusted_width

def save_results_to_excel(results):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for sheet_name, result_df in results.items():
            result_df.to_excel(writer, sheet_name=sheet_name, index=False)

            # Apply bold font to the total row
            worksheet = writer.sheets[sheet_name]
            total_row_idx = len(result_df)  # Total row index (1-based)
            for col in range(1, len(result_df.columns) + 2):  # Including index column
                cell = worksheet.cell(row=total_row_idx + 1, column=col)
                cell.font = Font(bold=True)
        
        autofit_excel(writer)
    output.seek(0)
    return output

def main():
    st.title("Excel File Comparison Tool")

    st.write("Upload two Excel files to compare them based on 'Ac Type Desc' column. The result will list common 'Ac Type Desc' with their balance sums and counts from both sheets.")

    previous_file = st.file_uploader("Upload First Excel File", type=["xlsx"])
    current_file = st.file_uploader("Upload Second Excel File", type=["xlsx"])

    if previous_file and current_file:
        with st.spinner("Processing..."):
            excel_sheets_1 = read_excel_sheets(previous_file)
            excel_sheets_2 = read_excel_sheets(current_file)

            results = calculate_common_actype_desc(excel_sheets_1, excel_sheets_2)

            output_file = save_results_to_excel(results)

        st.download_button(
            label="Download Comparison Output",
            data=output_file,
            file_name="comparison_output.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

if __name__ == "__main__":
    main()
