#%% ABOUT
'''
This code reads data files provided by the University of Liverpool and the
University of Bern and creates structured data tables for use in the
Antimicrobial Usage and Resistance (AMU/AMR) dashboard.

Contributors:
    Justin Replogle, First Analytics
    Kristen McCaffrey, First Analytics
'''
#%% IMPORTS & FUNCTIONS
# *****************************************************************************
import os, inspect
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import plotly.subplots as psub
import plotly.io as pio
pio.renderers.default='browser' 		# Plotly figures display best in browser

# To clean up column names in a dataframe
# Usage: mydata = clean_colnames(mydata)
def clean_colnames(INPUT_DF):
    # Comments inside the statement create errors. Putting all comments at the top.
    # Convert to lowercase
    # Strip leading and trailing spaces, then replace spaces with underscore
    # Replace slashes, parenthesis, and brackets with underscore
    # Replace some special characters with underscore
    # Replace other special characters with words
    dfmod = INPUT_DF
    dfmod.columns = dfmod.columns.astype(str)  # Convert to string
    dfmod.columns = dfmod.columns.str.lower() \
        .str.strip().str.replace(' ' ,'_' ,regex=False) \
        .str.replace('/' ,'_' ,regex=False).str.replace('\\' ,'_' ,regex=False) \
        .str.replace('(' ,'_' ,regex=False).str.replace(')' ,'_' ,regex=False) \
        .str.replace('[' ,'_' ,regex=False).str.replace(']' ,'_' ,regex=False) \
        .str.replace('{' ,'_' ,regex=False).str.replace('}' ,'_' ,regex=False) \
        .str.replace('!' ,'_' ,regex=False).str.replace('?' ,'_' ,regex=False) \
        .str.replace('-' ,'_' ,regex=False).str.replace('+' ,'_' ,regex=False) \
        .str.replace('^' ,'_' ,regex=False).str.replace('*' ,'_' ,regex=False) \
        .str.replace('.' ,'_' ,regex=False).str.replace(',' ,'_' ,regex=False) \
        .str.replace('|' ,'_' ,regex=False).str.replace('#' ,'_' ,regex=False) \
        .str.replace('>' ,'_gt_' ,regex=False) \
        .str.replace('<' ,'_lt_' ,regex=False) \
        .str.replace('=' ,'_eq_' ,regex=False) \
        .str.replace('@' ,'_at_' ,regex=False) \
        .str.replace('$' ,'_dol_' ,regex=False) \
        .str.replace('%' ,'_pct_' ,regex=False) \
        .str.replace('&' ,'_and_' ,regex=False)
    return dfmod

# To turn column index into names. Will remove multi-indexing.
# Usage: mydata = colnames_from_index(mydata)
def colnames_from_index(INPUT_DF):
    cols = list(INPUT_DF)
    cols_new = []
    for item in cols:
        if type(item) == str:   # Columns that already have names will be strings. Use unchanged.
            cols_new.append(item)
        else:           # Columns that are indexed or multi-indexed will appear as tuples. Turn them into strings joined by underscores.
            item_aslist = list(item)     # Convert tuple to list - necessary for next step
            if '' in item_aslist:
                item_aslist.remove('')     # Remove blanks
            cols_new.append('_'.join(str(i) for i in item))   # Convert each element of tuple to string before joining. Avoids error if an element is nan.

    # Write dataframe with new column names
    dfmod = INPUT_DF
    dfmod.columns = cols_new
    return dfmod

# To get the name of an object, as a string
# Usage: object_name = getobjectname(my_object)
def getobjectname(OBJECT):
    try:
        objectname = [x for x in globals() if globals()[x] is OBJECT][0]
    except:
        objectname = 'placeholder'
    return objectname

# To print df.info() with header for readability, and optionally write data info to text file
# Usage: datainfo(mydata)
def datainfo(
        INPUT_DF
        ,MAX_COLS:int=100         # Maximum number of columns to print out. Passed directly to max_cols argument of pandas df.info().
        ,OUTFOLDER:str=None     # Folder to output {dataname}_info.txt. None: no file will be created.
    ):
    funcname = inspect.currentframe().f_code.co_name
    dataname = getobjectname(INPUT_DF)

    rowcount = INPUT_DF.shape[0]
    colcount = INPUT_DF.shape[1]
    idxcols = str(list(INPUT_DF.index.names))
    header = f"Data name: {dataname :>26s}\nRows:      {rowcount :>26,}\nColumns:   {colcount :>26,}\nIndex:     {idxcols :>26s}\n"
    divider = ('-'*26) + ('-'*11) + '\n'
    bigdivider = ('='*26) + ('='*11) + '\n'
    print(bigdivider + header + divider)
    INPUT_DF.info(max_cols=MAX_COLS)
    print(divider + f"End:       {dataname:>26s}\n" + bigdivider)

    if OUTFOLDER:     # If something has been passed to OUTFOLDER parameter
        filename = f"{dataname}_info"
        print(f"\n<{funcname}> Creating file {OUTFOLDER}\\{filename}.txt")
        datetimestamp = 'Created on ' + time.strftime('%Y-%m-%d %X', time.gmtime()) + ' UTC' + '\n'
        buffer = io.StringIO()
        INPUT_DF.info(buf=buffer, max_cols=colcount)
        filecontents = header + divider + datetimestamp + buffer.getvalue()
        tofile = os.path.join(OUTFOLDER, f"{filename}.txt")
        with open(tofile, 'w', encoding='utf-8') as f:
            f.write(filecontents)
        print(f"<{funcname}> ...done.")
    return None

# To export a dataframe
# By default, creates both a pickle file and a CSV
# File name will be the name of the dataframe with either .pkl or .csv attached.
# Usage: export_dataframe(mydf, PRODATA_FOLDER)
def export_dataframe(
        DATAFRAME:object
        ,OUTPUT_FOLDER:str
        ,TO_PICKLE=True         # True: write a pickle file
        ,TO_CSV=True            # True: write a CSV
    ):
    funcname = inspect.currentframe().f_code.co_name

    dfname = [x for x in globals() if globals()[x] is DATAFRAME][0]

    if TO_PICKLE:
        outfile = os.path.join(OUTPUT_FOLDER, f"{dfname}.pkl.gz")
        DATAFRAME.to_pickle(outfile)
        print(f"\n<{funcname}> Dataframe {dfname} written to pickle file {outfile}.")

    if TO_CSV:
        outfile = os.path.join(OUTPUT_FOLDER, f"{dfname}.csv")
        DATAFRAME.to_csv(outfile, index=False)
        print(f"\n<{funcname}> Dataframe {dfname} written to CSV file {outfile}.")

    return None

#%% PATHS & CONSTANTS
# *****************************************************************************
# =============================================================================
#### Folder paths
# =============================================================================
try:
	CURRENT_FOLDER = os.path.dirname(os.path.realpath(__file__)) 	# Doesn't rely on working directory. Note will throw NameError if run as a single line. Works only when running a cell or the whole file.
	print(f"> CURRENT_FOLDER set to location of this file: {CURRENT_FOLDER}")
except NameError:
	CURRENT_FOLDER = os.getcwd() 		 			# Relies on working directory being set to this program.
	print(f"> CURRENT_FOLDER set to current working directory: {CURRENT_FOLDER}")

PARENT_FOLDER = os.path.dirname(CURRENT_FOLDER)
GRANDPARENT_FOLDER = os.path.dirname(PARENT_FOLDER)

RAWDATA_FOLDER = os.path.join(CURRENT_FOLDER, 'raw_data')
PRODATA_FOLDER = os.path.join(CURRENT_FOLDER, 'processed_data')
DASHDATA_FOLDER = os.path.join(GRANDPARENT_FOLDER, 'Dash App', 'data')

#%% DENMARK INITIAL DATA
# *****************************************************************************
# =============================================================================
#### Farm summary
# =============================================================================
den_farmsmry = pd.read_excel(
    os.path.join(RAWDATA_FOLDER, 'Denmark AMR data organizer JR.xlsx')
    ,sheet_name='Farm summary'
)
datainfo(den_farmsmry)
export_dataframe(den_farmsmry, PRODATA_FOLDER)

# =============================================================================
#### AMR
# =============================================================================
den_amr = pd.read_excel(
    os.path.join(RAWDATA_FOLDER, 'Denmark AMR data organizer JR.xlsx')
    ,sheet_name='AMR'
    ,header=[0,1]   # Header is split over two rows
)

# Cleanup column names
den_amr = colnames_from_index(den_amr)      # Flatten multi-indexed column names
den_amr = clean_colnames(den_amr)

rename_cols = {
    'unnamed:_0_level_0_scenario':"scenario"
    ,'unnamed:_1_level_0_farm_type':"farm_type"
    ,'unnamed:_2_level_0_number_of_farms':"number_of_farms"
    # ,'burden_of_amr_at_farm_level_median':""
    ,'burden_of_amr_at_farm_level_5_pct_ile':"burden_of_amr_at_farm_level_5pctile"
    ,'burden_of_amr_at_farm_level_95_pct_ile':"burden_of_amr_at_farm_level_95pctile"
    # ,'burden_of_amr_at_pop_level_median':""
    ,'burden_of_amr_at_pop_level_5_pct_ile':"burden_of_amr_at_pop_level_5pctile"
    ,'burden_of_amr_at_pop_level_95_pct_ile':"burden_of_amr_at_pop_level_95pctile"
    ,'health_expenditure_due_to_amr_diahorrea_amu_expenditure':"amr_health_exp_amu"
    ,'health_expenditure_due_to_amr_diahorrea_feed_corrections':"amr_health_exp_feed"
    ,'health_expenditure_due_to_amr_diahorrea_production_losses':"amr_health_exp_prod"
}
den_amr = den_amr.rename(columns=rename_cols)

# Make AMR numbers positive for graphing
numeric_cols = list(den_amr.select_dtypes('number'))
for COL in numeric_cols:
    den_amr[COL] = abs(den_amr[COL])

datainfo(den_amr)
export_dataframe(den_amr, PRODATA_FOLDER)

# =============================================================================
#### AHLE
# =============================================================================
den_ahle = pd.read_excel(
    os.path.join(RAWDATA_FOLDER, 'Denmark AMR data organizer JR.xlsx')
    ,sheet_name='AHLE'
)
den_ahle = clean_colnames(den_ahle)

rename_cols = {
    'ahle_at_farm_level___median':'ahle_at_farm_level_median'
    ,'ahle_at_farm_level___5_pct_ile':'ahle_at_farm_level_5pctile'
    ,'ahle_at_farm_level___95_pct_ile':'ahle_at_farm_level_95pctile'
    ,'ahle_at_pop_level___median':'ahle_at_pop_level_median'
    ,'ahle_at_pop_level___5_pct_ile':'ahle_at_pop_level_5pctile'
    ,'ahle_at_pop_level___95_pct_ile':'ahle_at_pop_level_95pctile'
}
den_ahle = den_ahle.rename(columns=rename_cols)

# Make AHLE numbers positive for graphing
numeric_cols = list(den_ahle.select_dtypes('number'))
for COL in numeric_cols:
    den_ahle[COL] = abs(den_ahle[COL])

datainfo(den_ahle)
export_dataframe(den_ahle, PRODATA_FOLDER)

# =============================================================================
#### AMR and AHLE combo
# =============================================================================
den_amr_ahle = pd.merge(
    left=den_amr
    ,right=den_ahle.drop(columns='number_of_farms')
    ,on='farm_type'
    ,how='left'
)

# Add calcs
# When calculating AMR as a proportion of AHLE for the 5th and 95th percentiles, Joao's
# spreadsheet varies the denominator (AHLE) in addition to the numerator. I'm doing the same here.
# An alternative would be to keep the median AHLE as the denominator and just vary the numerator (AMR).
# This represents the uncertainty in AMR without conflating it with uncertainty in the AHLE.
den_amr_ahle = den_amr_ahle.eval(
    # Farm level
    '''
    ahle_at_farm_level_median_withoutamr = ahle_at_farm_level_median - burden_of_amr_at_farm_level_median
    ahle_at_farm_level_median_errhigh = ahle_at_farm_level_5pctile - ahle_at_farm_level_median
    ahle_at_farm_level_median_errlow = ahle_at_farm_level_median - ahle_at_farm_level_95pctile

    burden_of_amr_at_farm_level_median_pctofahle = burden_of_amr_at_farm_level_median / ahle_at_farm_level_median
    burden_of_amr_at_farm_level_5pctile_pctofahle = burden_of_amr_at_farm_level_5pctile / ahle_at_farm_level_5pctile
    burden_of_amr_at_farm_level_95pctile_pctofahle = burden_of_amr_at_farm_level_95pctile / ahle_at_farm_level_95pctile

    burden_of_amr_at_farm_level_errhigh = burden_of_amr_at_farm_level_5pctile - burden_of_amr_at_farm_level_median
    burden_of_amr_at_farm_level_errlow = burden_of_amr_at_farm_level_median - burden_of_amr_at_farm_level_95pctile
    '''
    # Population level
    '''
    ahle_at_pop_level_median_withoutamr = ahle_at_pop_level_median - burden_of_amr_at_pop_level_median
    ahle_at_pop_level_median_errhigh = ahle_at_pop_level_5pctile - ahle_at_pop_level_median
    ahle_at_pop_level_median_errlow = ahle_at_pop_level_median - ahle_at_pop_level_95pctile

    burden_of_amr_at_pop_level_median_pctofahle = burden_of_amr_at_pop_level_median / ahle_at_pop_level_median
    burden_of_amr_at_pop_level_5pctile_pctofahle = burden_of_amr_at_pop_level_5pctile / ahle_at_pop_level_5pctile
    burden_of_amr_at_pop_level_95pctile_pctofahle = burden_of_amr_at_pop_level_95pctile / ahle_at_pop_level_95pctile

    burden_of_amr_at_pop_level_errhigh = burden_of_amr_at_pop_level_5pctile - burden_of_amr_at_pop_level_median
    burden_of_amr_at_pop_level_errlow = burden_of_amr_at_pop_level_median - burden_of_amr_at_pop_level_95pctile
    '''
)
datainfo(den_amr_ahle)
export_dataframe(den_amr_ahle, PRODATA_FOLDER)
export_dataframe(den_amr_ahle, DASHDATA_FOLDER)

# -----------------------------------------------------------------------------
#### -- Reshape for plotting - farm level
# -----------------------------------------------------------------------------
# Median estimates
den_amr_ahle_farmlvl = den_amr_ahle.melt(
	id_vars=['scenario', 'farm_type', 'number_of_farms']
	,value_vars=[
        'burden_of_amr_at_farm_level_median'
        ,'ahle_at_farm_level_median_withoutamr'
    ]
	,var_name='metric'
	,value_name='value'
)

# Errors
den_amr_ahle_farmlvl["error_high"] = den_amr_ahle[["burden_of_amr_at_farm_level_errhigh", "ahle_at_farm_level_median_errhigh"]].unstack().values
den_amr_ahle_farmlvl["error_low"] = den_amr_ahle[["burden_of_amr_at_farm_level_errlow", "ahle_at_farm_level_median_errlow"]].unstack().values

export_dataframe(den_amr_ahle_farmlvl, PRODATA_FOLDER)
export_dataframe(den_amr_ahle_farmlvl, DASHDATA_FOLDER)

# -----------------------------------------------------------------------------
#### -- Reshape for plotting - population level
# -----------------------------------------------------------------------------
# Median estimates
den_amr_ahle_poplvl = den_amr_ahle.melt(
	id_vars=['scenario', 'farm_type', 'number_of_farms']
	,value_vars=[
        'burden_of_amr_at_pop_level_median'
        ,'ahle_at_pop_level_median_withoutamr'
    ]
	,var_name='metric'
	,value_name='value'
)

# Errors
den_amr_ahle_poplvl["error_high"] = den_amr_ahle[["burden_of_amr_at_pop_level_errhigh", "ahle_at_pop_level_median_errhigh"]].unstack().values
den_amr_ahle_poplvl["error_low"] = den_amr_ahle[["burden_of_amr_at_pop_level_errlow", "ahle_at_pop_level_median_errlow"]].unstack().values

export_dataframe(den_amr_ahle_poplvl, PRODATA_FOLDER)
export_dataframe(den_amr_ahle_poplvl, DASHDATA_FOLDER)

# =============================================================================
#### DEV Code: Fixing hidden error bars for stacked bar chart
# =============================================================================
input_df = den_amr_ahle_poplvl.query("scenario == 'Average'").query("farm_type != 'Total'").copy()

# Define custom sort order for columns
scenario_order = ['Average', 'Worse', 'Best']
farm_type_order = ['Breed', 'Nurse', 'Fat', 'Total']
metric_order = ['burden_of_amr_at_pop_level_median', 'ahle_at_pop_level_median_withoutamr']

input_df['scenario'] = pd.Categorical(input_df['scenario'], categories=scenario_order, ordered=True)
input_df['farm_type'] = pd.Categorical(input_df['farm_type'], categories=farm_type_order, ordered=True)
input_df['metric'] = pd.Categorical(input_df['metric'], categories=metric_order, ordered=True)

# Calculate cumulative values for plotly trick to overlay error bars
input_df = input_df.sort_values(['scenario', 'farm_type', 'metric']).reset_index(drop=True)
input_df['cumluative_value_over_metrics'] = input_df.groupby('farm_type')['value'].cumsum()

# Create trace for each farm type
traces = []
unique_metrics = input_df['metric'].unique()

for i, selected_metric in enumerate(unique_metrics):
    traces.append(go.Bar(
        name=selected_metric,
        x=input_df.query(f"metric == '{selected_metric}'")['farm_type'],
        y=input_df.query(f"metric == '{selected_metric}'")['value'],
        marker_color=['#31BFF3', '#fbc98e'][i],   # Different color for each metric
    ))

# Add error bars
for i, selected_metric in enumerate(unique_metrics):
    traces.append(go.Scatter(
        name=f"{selected_metric}_error",
        x=input_df.query(f"metric == '{selected_metric}'")['farm_type'],
        y=input_df.query(f"metric == '{selected_metric}'")['cumluative_value_over_metrics'],
        mode='markers',
        marker=dict(color='gray'),
        error_y=dict(
            type='data',
            array=input_df.query(f"metric == '{selected_metric}'")['error_high'],
            arrayminus=input_df.query(f"metric == '{selected_metric}'")['error_low'],
            visible=True,
            color='gray',
            thickness=2,
            width=5
        ),
        showlegend=False,
    ))

# Create the layout
layout = go.Layout(
    title='AMR in the context of AHLE',
    barmode='stack',
    xaxis={'title': 'Farm Type'},
    yaxis={
        'type':'log',
        'title':'Burden (DKK)',
    },
    legend_title='Source of Burden',
    template='plotly_white',
    height=600,
    width=800
)

# Create figure and show
fig = go.Figure(data=traces, layout=layout)
fig.show()

# =============================================================================
#### AHLE details from Bern
# =============================================================================
den_ahle_dtl = pd.read_excel(
    os.path.join(RAWDATA_FOLDER, 'Denmark AMR data organizer JR.xlsx')
    ,sheet_name='AHLE from U Bern'
)
den_ahle_dtl = clean_colnames(den_ahle_dtl)
datainfo(den_ahle_dtl)
export_dataframe(den_ahle_dtl, PRODATA_FOLDER)

# =============================================================================
#### AHLE inputs from Bern
# =============================================================================
den_ahle_inputs = pd.read_excel(
    os.path.join(RAWDATA_FOLDER, 'Denmark AMR data organizer JR.xlsx')
    ,sheet_name='Inputs from U Bern'
)
den_ahle_inputs = clean_colnames(den_ahle_inputs)
datainfo(den_ahle_inputs)
export_dataframe(den_ahle_inputs, PRODATA_FOLDER)

# =============================================================================
#### AHLE scenario comparisons from Bern
# =============================================================================
den_ahle_comp = pd.read_excel(
    os.path.join(RAWDATA_FOLDER, 'Denmark AMR data organizer JR.xlsx')
    ,sheet_name='Scenario comps from U Bern'
    ,header=[0,1]   # Header is split over two rows
)
den_ahle_comp = colnames_from_index(den_ahle_comp)      # Flatten multi-indexed column names
den_ahle_comp = clean_colnames(den_ahle_comp)

rename_cols = {
    "all_columns_are_differences_per_year_scenario":"scenario"
    ,"all_columns_are_differences_per_year_farm_type":"farm_type"
    ,"farm_delta_variable_costs___delta_gm":"farm_delta_variable_costs_prpn_delta_gm"
}
den_ahle_comp = den_ahle_comp.rename(columns=rename_cols)

datainfo(den_ahle_comp)
export_dataframe(den_ahle_comp, PRODATA_FOLDER)

#%% DENMARK DATA END OF FEBRUARY
# *****************************************************************************
'''
This is the data Joao shared at the end of February which is the final result
using their existing attribution method. We may or may not get an updated data
set from their ongoing work to update their methods.

NO LONGER USED. JOAO SENT A CORRECTED FILE ON MARCH 5.
'''
# # -----------------------------------------------------------------------------
# # Results from expert opinion method
# # -----------------------------------------------------------------------------
# den_amr_final_eo = pd.read_excel(
#     os.path.join(RAWDATA_FOLDER, 'Results Denmark.xlsx')
#     ,sheet_name='Expert opinion'
#     ,header=[1, 2]
# )

# # Cleanup column names
# den_amr_final_eo = colnames_from_index(den_amr_final_eo)
# den_amr_final_eo = clean_colnames(den_amr_final_eo)
# rename_cols = {
#     "item_unnamed:_0_level_1":"item"
#     ,"scenario_unnamed:_1_level_1":"scenario"
# }
# den_amr_final_eo = den_amr_final_eo.rename(columns=rename_cols)

# # Separate AHLE and expenditure numbers
# _row_select = (den_amr_final_eo['item'].str.upper().isin(['AHLE' ,'EXPENDITURE DKK']))
# den_amr_final_eo_ahle = den_amr_final_eo.loc[_row_select]
# den_amr_final_eo_amr = den_amr_final_eo.loc[~ _row_select]

# # Transpose item names
# den_amr_final_eo_amr_p = den_amr_final_eo_amr.pivot(
# 	index=['scenario']       # Column(s) to make new index. If blank, uses existing index.
# 	,columns=['item']        # Column(s) whose values define new columns
# )
# den_amr_final_eo_amr_p = colnames_from_index(den_amr_final_eo_amr_p)
# den_amr_final_eo_amr_p = den_amr_final_eo_amr_p.reset_index()
# den_amr_final_eo_amr_p = clean_colnames(den_amr_final_eo_amr_p)

# datainfo(den_amr_final_eo_amr_p)

# # -----------------------------------------------------------------------------
# # Results from internal method
# # -----------------------------------------------------------------------------
# den_amr_final = pd.read_excel(
#     os.path.join(RAWDATA_FOLDER, 'Results Denmark.xlsx')
#     ,sheet_name='Our method'
#     ,header=[1, 2]   # Header is split over two rows
# )

#%% DENMARK DATA MARCH 5
# *****************************************************************************
# =============================================================================
#### AMR from UoL
# =============================================================================
# -----------------------------------------------------------------------------
#### -- Import and basic column cleanup
# -----------------------------------------------------------------------------
den_amr_final = pd.read_excel(
    os.path.join(RAWDATA_FOLDER, 'Denmark AMR data organizer JR - March 5 update.xlsx')
    ,sheet_name='AMR'
    ,header=[0,1]   # Header is split over two rows
)

# Cleanup column names
den_amr_final = colnames_from_index(den_amr_final)      # Flatten multi-indexed column names
den_amr_final = clean_colnames(den_amr_final)

rename_cols = {
    'unnamed:_0_level_0_scenario':"scenario"
    ,'unnamed:_1_level_0_farm_type':"farm_type"
    ,'unnamed:_2_level_0_number_of_farms':"number_of_farms"
}
den_amr_final = den_amr_final.rename(columns=rename_cols)

# -----------------------------------------------------------------------------
#### -- Cleanup
# -----------------------------------------------------------------------------
# AMR numbers are reported as negative (seen in the median values; confidence limits can be positive)
# To be consistent with AHLE, flip the sign of AMR estimates
# Note this means flipping the upper and lower confidence levels (upper becomes lower and vice versa)
# Note also that some confidence intervals contain zero (upper and lower limits have opposite sign), so can't use abs(), must use *(-1)
reverse_sign_for_vars = [
    'amr_production_losses_at_farm_level_median'
    ,'amr_production_losses_at_farm_level_5_pct_ile'
    ,'amr_production_losses_at_farm_level_95_pct_ile'

    ,'amr_production_losses_at_pop_level_median'
    ,'amr_production_losses_at_pop_level_5_pct_ile'
    ,'amr_production_losses_at_pop_level_95_pct_ile'

    ,'amr_health_expenditure_at_pop_level_median'
    ,'amr_health_expenditure_at_pop_level_5_pct_ile'
    ,'amr_health_expenditure_at_pop_level_95_pct_ile'

    ,'amr_total_burden_at_pop_level_median'
    ,'amr_total_burden_at_pop_level_5_pct_ile'
    ,'amr_total_burden_at_pop_level_95_pct_ile'
]
for COL in reverse_sign_for_vars:
    den_amr_final[COL] = den_amr_final[COL] * -1

# Dictionary to define pairs of variables to swap (upper and lower confidence limits)
exchange_vars = {
    'amr_production_losses_at_farm_level_5_pct_ile':'amr_production_losses_at_farm_level_95_pct_ile'
    ,'amr_production_losses_at_pop_level_5_pct_ile':'amr_production_losses_at_pop_level_95_pct_ile'
    ,'amr_health_expenditure_at_pop_level_5_pct_ile':'amr_health_expenditure_at_pop_level_95_pct_ile'
    ,'amr_total_burden_at_pop_level_5_pct_ile':'amr_total_burden_at_pop_level_95_pct_ile'
}
for KEY, VALUE in exchange_vars.items():
    den_amr_final = den_amr_final.rename(columns={KEY:VALUE, VALUE:KEY})

# -----------------------------------------------------------------------------
#### -- Fill in health expenditure for each farm type
# Allocate population health expenditure to each type in the same proportion as production losses
# -----------------------------------------------------------------------------
'''
3/20: Sara responded to my question about this. Sounds like I'm misinterpreting the data.
'''
# # Get proportion of population production losses in each farm type, separately for each scenario
# keep_cols = [
#     'scenario'
#     ,'farm_type'
#     ,'amr_production_losses_at_pop_level_median'        # To get each farm types proportion of total
#     # ,'amr_production_losses_at_pop_level_5_pct_ile'
#     # ,'amr_production_losses_at_pop_level_95_pct_ile'
#     ,'amr_health_expenditure_at_pop_level_median'       # To allocate to farm types
#     ,'amr_health_expenditure_at_pop_level_5_pct_ile'    # To allocate to farm types
#     ,'amr_health_expenditure_at_pop_level_95_pct_ile'   # To allocate to farm types
# ]
# totals_by_scenario = den_amr_final.query("farm_type == 'Total'")[keep_cols]
# totals_by_scenario = totals_by_scenario.set_index(keys=['scenario', 'farm_type'])
# totals_by_scenario = totals_by_scenario.add_prefix('total_')
# datainfo(totals_by_scenario)

# den_amr_final_working = pd.merge(
#     left=den_amr_final
#     ,right=totals_by_scenario
#     ,how='left'
#     ,on='scenario'
# )
# datainfo(den_amr_final_working)

# # Calculate health expenditure for each farm type by allocating population total proportionally
# den_amr_final_working = den_amr_final_working.eval(
#     f'''
#     prpn_prodloss_this_farm_type = amr_production_losses_at_pop_level_median / total_amr_production_losses_at_pop_level_median

#     amr_health_expenditure_prpn_median = total_amr_health_expenditure_at_pop_level_median * prpn_prodloss_this_farm_type
#     amr_health_expenditure_prpn_5pct = total_amr_health_expenditure_at_pop_level_5_pct_ile * prpn_prodloss_this_farm_type
#     amr_health_expenditure_prpn_95pct = total_amr_health_expenditure_at_pop_level_95_pct_ile * prpn_prodloss_this_farm_type
#     '''
# )

# # Fill in missing health expenditure with calculated value
# # Dictionary with KEY: column with missings to fill, VALUE: column with values to use
# fill_na = {
#     'amr_health_expenditure_at_pop_level_median':'amr_health_expenditure_prpn_median'
#     ,'amr_health_expenditure_at_pop_level_5_pct_ile':'amr_health_expenditure_prpn_5pct'
#     ,'amr_health_expenditure_at_pop_level_95_pct_ile':'amr_health_expenditure_prpn_95pct'
# }
# for BASE_COL, FILL_COL in fill_na.items():
#     den_amr_final_working[BASE_COL] = den_amr_final_working[BASE_COL].fillna(den_amr_final_working[FILL_COL])

# # Recalculate total burden using new health expenditure
# den_amr_final_working = den_amr_final_working.eval(
#     f'''
#     amr_total_burden_at_pop_level_median = amr_production_losses_at_pop_level_median + amr_health_expenditure_at_pop_level_median
#     amr_total_burden_at_pop_level_5_pct_ile = amr_production_losses_at_pop_level_5_pct_ile + amr_health_expenditure_at_pop_level_5_pct_ile
#     amr_total_burden_at_pop_level_95_pct_ile = amr_production_losses_at_pop_level_95_pct_ile + amr_health_expenditure_at_pop_level_95_pct_ile
#     '''
# )

# # Trim working columns and replace original data
# orig_cols = list(den_amr_final)
# den_amr_final = den_amr_final_working[orig_cols].copy()

# -----------------------------------------------------------------------------
#### -- Fill in total burden for each farm type
# -----------------------------------------------------------------------------
'''
Based on Sara's comment 3/20, Total AMR Burden by farm type is equal to
production losses (there is no separate health expenditure).
'''
_non_total = (den_amr_final['farm_type'] != 'Total')

den_amr_final.loc[_non_total, 'amr_total_burden_at_pop_level_median'] = \
    den_amr_final.loc[_non_total, 'amr_production_losses_at_pop_level_median']

den_amr_final.loc[_non_total, 'amr_total_burden_at_pop_level_95_pct_ile'] = \
    den_amr_final.loc[_non_total, 'amr_production_losses_at_pop_level_95_pct_ile']

den_amr_final.loc[_non_total, 'amr_total_burden_at_pop_level_5_pct_ile'] = \
    den_amr_final.loc[_non_total, 'amr_production_losses_at_pop_level_5_pct_ile']

# -----------------------------------------------------------------------------
#### -- Export
# -----------------------------------------------------------------------------
datainfo(den_amr_final)
export_dataframe(den_amr_final, PRODATA_FOLDER)

# =============================================================================
#### AHLE from UoL
# =============================================================================
den_ahle_final = pd.read_excel(
    os.path.join(RAWDATA_FOLDER, 'Denmark AMR data organizer JR - March 5 update.xlsx')
    ,sheet_name='AHLE'
)
den_ahle_final = clean_colnames(den_ahle_final)

# # Make AHLE numbers positive for graphing
# numeric_cols = list(den_ahle_final.select_dtypes('number'))
# for COL in numeric_cols:
#     den_ahle_final[COL] = abs(den_ahle_final[COL])

datainfo(den_ahle_final)
export_dataframe(den_ahle_final, PRODATA_FOLDER)

# =============================================================================
#### AHLE from Bern
# =============================================================================
# Full version
#!!! Clean this up for display in the dashboard
den_ahle_bern_final = pd.read_excel(
    os.path.join(RAWDATA_FOLDER, 'Results AHLE Dashboard.xlsx')
    ,sheet_name='Results AHLE Dashboard'
)
den_ahle_bern_final = clean_colnames(den_ahle_bern_final)

# Trimmed version from data organizer
den_ahle_bern_final_jr = pd.read_excel(
    os.path.join(RAWDATA_FOLDER, 'Denmark AMR data organizer JR - March 5 update.xlsx')
    ,sheet_name='AHLE from U Bern'
)
den_ahle_bern_final_jr = clean_colnames(den_ahle_bern_final_jr)

# =============================================================================
#### AMR and AHLE combo
# =============================================================================
# -----------------------------------------------------------------------------
#### -- Combine
# -----------------------------------------------------------------------------
# Using AHLE from UoL - not broken out by farm type!
# den_amr_ahle_final = pd.merge(
#     left=den_amr_final
#     ,right=den_ahle_final.drop(columns='number_of_farms')
#     ,on='farm_type'
#     ,how='left'
# )

# Using AHLE from Bern
recode_farmtype = {
    "Breed":"Breeding"
    ,"Nurse":"Rearing"
    ,"Fat":"Fattening"
    ,"Total":"TOTAL"
}
den_amr_final['production_stage'] = den_amr_final['farm_type'].replace(recode_farmtype)
den_amr_ahle_final = pd.merge(
    left=den_amr_final
    ,right=den_ahle_bern_final_jr.drop(columns=['number_of_farms_affected', 'delta_gm_per_farm'])
    ,on='production_stage'
    ,how='left'
)
datainfo(den_amr_ahle_final)

# Add calcs
# Note: here is where it matters if the estimates are positive or negative - this determines order of terms for subtraction to calc error_high or error_low
# If medians are positive, error_high = 95%ile minus median and error_low = median minus 5%ile
den_amr_ahle_final = den_amr_ahle_final.eval(
    # Farm level AHLE not available in latest data
    # Population level
    '''
    ahle_at_pop_level_withoutamr_median = population_ahle_median - amr_total_burden_at_pop_level_median
    ahle_at_pop_level_withoutamr_errhigh = population_ahle_95_pct__percentile - population_ahle_median
    ahle_at_pop_level_withoutamr_errlow =  population_ahle_median - population_ahle_5_pct__percentile

    amr_production_losses_at_pop_level_errhigh = amr_production_losses_at_pop_level_95_pct_ile - amr_production_losses_at_pop_level_median
    amr_production_losses_at_pop_level_errlow = amr_production_losses_at_pop_level_median - amr_production_losses_at_pop_level_5_pct_ile

    amr_health_expenditure_at_pop_level_errhigh = amr_health_expenditure_at_pop_level_95_pct_ile - amr_health_expenditure_at_pop_level_median
    amr_health_expenditure_at_pop_level_errlow = amr_health_expenditure_at_pop_level_median - amr_health_expenditure_at_pop_level_5_pct_ile

    amr_total_burden_at_pop_level_errhigh = amr_total_burden_at_pop_level_95_pct_ile - amr_total_burden_at_pop_level_median
    amr_total_burden_at_pop_level_errlow = amr_total_burden_at_pop_level_median - amr_total_burden_at_pop_level_5_pct_ile

    amr_total_burden_at_pop_level_median_pctofahle = amr_total_burden_at_pop_level_median / population_ahle_median
    amr_total_burden_at_pop_level_5pctile_pctofahle = amr_total_burden_at_pop_level_5_pct_ile / population_ahle_median
    amr_total_burden_at_pop_level_95pctile_pctofahle = amr_total_burden_at_pop_level_95_pct_ile / population_ahle_median
    '''
)
export_dataframe(den_amr_ahle_final, PRODATA_FOLDER)
export_dataframe(den_amr_ahle_final, DASHDATA_FOLDER)
datainfo(den_amr_ahle_final)

# -----------------------------------------------------------------------------
#### -- Reshape for plotting - population level
# -----------------------------------------------------------------------------
# Melt and merge relies on consistent column ordering
columns_inorder = [
    'amr_production_losses_at_pop_level'
    ,'amr_health_expenditure_at_pop_level'
    ,'amr_total_burden_at_pop_level'  # This is the sum of AMR production losses and AMR health expenditure.
    ,'ahle_at_pop_level_withoutamr'
]
columns_inorder_median = [COL + '_median' for COL in columns_inorder]
columns_inorder_errhigh = [COL + '_errhigh' for COL in columns_inorder]
columns_inorder_errlow = [COL + '_errlow' for COL in columns_inorder]

# Medians
den_amr_ahle_final_poplvl_median = den_amr_ahle_final.melt(
	id_vars=['scenario', 'farm_type', 'number_of_farms']
	,value_vars=columns_inorder_median
	,var_name='metric'
	,value_name='value'
)

# Errors
den_amr_ahle_final_poplvl_errhigh = den_amr_ahle_final.melt(
	id_vars=['scenario', 'farm_type', 'number_of_farms']
	,value_vars=columns_inorder_errhigh
	,var_name='metric'
	,value_name='error_high'
)
den_amr_ahle_final_poplvl_errlow = den_amr_ahle_final.melt(
	id_vars=['scenario', 'farm_type', 'number_of_farms']
	,value_vars=columns_inorder_errlow
	,var_name='metric'
	,value_name='error_low'
)

# Put them together
den_amr_ahle_final_poplvl = den_amr_ahle_final_poplvl_median.copy()
den_amr_ahle_final_poplvl['error_high'] = den_amr_ahle_final_poplvl_errhigh['error_high']
den_amr_ahle_final_poplvl['error_low'] = den_amr_ahle_final_poplvl_errlow['error_low']

# -----------------------------------------------------------------------------
#### -- Add exchange rate
# -----------------------------------------------------------------------------
'''
From Sara 3/13/2025:
    For our Danish dashboard, please use the exchange rate 1 Danish Krone = 0.1416 US Dollar (average exchange rate for 2022).
'''
usd_per_dkk = 0.1416

rename_cols = {
    "value":"value_dkk"
    ,"error_high":"error_high_dkk"
    ,"error_low":"error_low_dkk"
}
den_amr_ahle_final_poplvl = den_amr_ahle_final_poplvl.rename(columns=rename_cols)

den_amr_ahle_final_poplvl['value_usd'] = den_amr_ahle_final_poplvl['value_dkk'] * usd_per_dkk
den_amr_ahle_final_poplvl['error_high_usd'] = den_amr_ahle_final_poplvl['error_high_dkk'] * usd_per_dkk
den_amr_ahle_final_poplvl['error_low_usd'] = den_amr_ahle_final_poplvl['error_low_dkk'] * usd_per_dkk

# -----------------------------------------------------------------------------
#### -- Add biomass
# -----------------------------------------------------------------------------
'''
#!!! From Beat 3/13:
    -Slaughter weight for DK is 114.7kg
    -17,203,200 are slaughtered in DK
    -> Total of 1,973,207,040 kg

Update 3/24: Beat will incorporate average weight for SOWs to get a new total
'''

# -----------------------------------------------------------------------------
#### -- Export
# -----------------------------------------------------------------------------
datainfo(den_amr_ahle_final_poplvl)
export_dataframe(den_amr_ahle_final_poplvl, PRODATA_FOLDER)
export_dataframe(den_amr_ahle_final_poplvl, DASHDATA_FOLDER)

# =============================================================================
#### Selected input parameters
# =============================================================================
'''
All parameters for AMR modeling are in the following file provided by UoL:
    AMR attribution PWD DK Feb2025_old_method_ideal_v1.0.xlsx

The current plan is not to show these, but we will use the incidence rates to
label the dashboard selector for scenario (average, worst, best):
    RiskPert(0.0065,0.0736,0.1942)
'''
#%% ETHIOPIA DATA MARCH 5
# *****************************************************************************
# =============================================================================
#### AMR from UoL
# =============================================================================
# -----------------------------------------------------------------------------
#### -- Import and basic column cleanup
# -----------------------------------------------------------------------------
eth_amr_imp = pd.read_excel(
    os.path.join(RAWDATA_FOLDER, 'results_ethiopia_5March25_JREdit.xlsx')
    ,sheet_name='overall'
    ,usecols='A:E'
)
eth_amr_imp = clean_colnames(eth_amr_imp)
datainfo(eth_amr_imp)

# -----------------------------------------------------------------------------
#### -- Bash into shape
# Want same columns as Denmark data to use same plotting code
# -----------------------------------------------------------------------------
# Keep the metrics we want - USD, not billions
keep_metrics = [
    'Production losses due to mastitis (USD)'
    ,'Production losses due to resistant mastitis (USD)'
    ,'Expenditure with mastitis (USD)'
    ,'Expenditure with resistant mastitis (USD)'
    ,'Indirect costs due to AMR (USD)'
    ,'AHLE - cattle (USD)'
    ,'Expenditure in cattle (USD)'
    ,'Total AMR burden (USD)'
    ,'AHLE without AMR (USD)'
]
_row_select = (eth_amr_imp['metric'].isin(keep_metrics))
eth_amr = eth_amr_imp.loc[_row_select].copy()

# Calc point estimate and error limits
eth_amr = eth_amr.eval(
    f'''
    value_usd = mean
    error_high_usd = upper_95_pct__ci - mean
    error_low_usd = mean - lower_95_pct__ci
    upper_95pct_ci_usd = upper_95_pct__ci
    lower_95pct_ci_usd = lower_95_pct__ci
    '''
)
eth_amr = eth_amr.drop(columns=['median', 'mean', 'upper_95_pct__ci', 'lower_95_pct__ci'])

# Currency is in column name, drop from metric
eth_amr['metric'] = eth_amr['metric'].str.replace(' (USD)', '', regex=False)

# Add a dummy column for production system - all one value
eth_amr['production_system'] = 'Overall'

# -----------------------------------------------------------------------------
#### -- Add exchange rate
# -----------------------------------------------------------------------------
# Bring in 2021 rate from Ethiopia dashboard. Confirm with Joao that this is the correct year.
# https://data.worldbank.org/indicator/PA.NUS.FCRF?end=2023&start=2023&view=bar
birr_per_usd_2023 = 54.60
birr_per_usd_2022 = 51.76
birr_per_usd_2021 = 43.73
birr_per_usd_2020 = 34.93

eth_amr['value_birr'] = eth_amr['value_usd'] * birr_per_usd_2021
eth_amr['error_high_birr'] = eth_amr['error_high_usd'] * birr_per_usd_2021
eth_amr['error_low_birr'] = eth_amr['error_low_usd'] * birr_per_usd_2021
eth_amr['upper_95pct_ci_birr'] = eth_amr['upper_95pct_ci_usd'] * birr_per_usd_2021
eth_amr['lower_95pct_ci_birr'] = eth_amr['lower_95pct_ci_usd'] * birr_per_usd_2021

# -----------------------------------------------------------------------------
#### -- Export
# -----------------------------------------------------------------------------
datainfo(eth_amr)
export_dataframe(eth_amr, PRODATA_FOLDER)
export_dataframe(eth_amr, DASHDATA_FOLDER)

# =============================================================================
#### Data by Production System
# =============================================================================
# -----------------------------------------------------------------------------
#### -- AMR by Production System
# -----------------------------------------------------------------------------
eth_amr_prodsys_imp = pd.read_excel(
    os.path.join(RAWDATA_FOLDER, 'results_ethiopia_5March25_JREdit.xlsx')
    ,sheet_name='AHLE and burden per prod syst'
)
eth_amr_prodsys_imp = clean_colnames(eth_amr_prodsys_imp)
datainfo(eth_amr_prodsys_imp)

# Pivot metrics into columns
eth_amr_prodsys_imp['combined_metric'] = eth_amr_prodsys_imp['attributable_burden'].astype(str) + '_' + eth_amr_prodsys_imp['parameter']
eth_amr_prodsys_p = eth_amr_prodsys_imp.pivot(
	index='production_system'
	,columns='combined_metric'
# 	,values=['min_', '5th_centile', '1st_qu_', 'median', 'mean', '3rd_qu_', '95th_centile', 'max_']
	,values=['5th_centile', 'median', 'mean', '95th_centile']
)
eth_amr_prodsys_p = colnames_from_index(eth_amr_prodsys_p)
eth_amr_prodsys_p = eth_amr_prodsys_p.reset_index()
eth_amr_prodsys_p = clean_colnames(eth_amr_prodsys_p)

# Add total row
# eth_amr_prodsys_p.loc['column_total'] = eth_amr_prodsys_p.sum(numeric_only=True, axis=0)
# eth_amr_prodsys_p.loc['column_total', 'production_system'] = 'Overall'
# eth_amr_prodsys_p = eth_amr_prodsys_p.reset_index(drop=True)

datainfo(eth_amr_prodsys_p)

# Add calcs
## Note these require df['col'] syntax due to column names starting with numbers
eth_amr_prodsys_p['amr_production_losses_median_usd'] = (eth_amr_prodsys_p['median_antimicrobial_resistant_in_mastitis_morbidity__billion_us_dol__'] + eth_amr_prodsys_p['median_antimicrobial_resistant_in_mastitis_mortality__billion_us_dol__']) * 1e9
eth_amr_prodsys_p['amr_production_losses_5pctl_usd'] = (eth_amr_prodsys_p['5th_centile_antimicrobial_resistant_in_mastitis_morbidity__billion_us_dol__'] + eth_amr_prodsys_p['5th_centile_antimicrobial_resistant_in_mastitis_mortality__billion_us_dol__']) * 1e9
eth_amr_prodsys_p['amr_production_losses_95pctl_usd'] = (eth_amr_prodsys_p['95th_centile_antimicrobial_resistant_in_mastitis_morbidity__billion_us_dol__'] + eth_amr_prodsys_p['95th_centile_antimicrobial_resistant_in_mastitis_mortality__billion_us_dol__']) * 1e9

# Note "unattributed ahle" in this data matches total AHLE in overall data, so I'm naming it as such
eth_amr_prodsys_p['total_ahle_mean_usd'] = eth_amr_prodsys_p['mean_unnatributted_ahle__billion_us_dol__'] * 1e9
eth_amr_prodsys_p['total_ahle_5pctl_usd'] = eth_amr_prodsys_p['5th_centile_unnatributted_ahle__billion_us_dol__'] * 1e9
eth_amr_prodsys_p['total_ahle_95pctl_usd'] = eth_amr_prodsys_p['95th_centile_unnatributted_ahle__billion_us_dol__'] * 1e9

# Calcs 2
eth_amr_prodsys_p = eth_amr_prodsys_p.eval(
    f'''
    ahle_withoutamr_mean_usd = total_ahle_mean_usd - amr_production_losses_median_usd
    ahle_withoutamr_errhigh_usd = total_ahle_95pctl_usd - total_ahle_mean_usd
    ahle_withoutamr_errlow_usd = total_ahle_mean_usd - total_ahle_5pctl_usd

    amr_production_losses_errhigh_usd = amr_production_losses_95pctl_usd - amr_production_losses_median_usd
    amr_production_losses_errlow_usd = amr_production_losses_median_usd - amr_production_losses_5pctl_usd
    '''
)

# -----------------------------------------------------------------------------
#### -- Export
# -----------------------------------------------------------------------------
datainfo(eth_amr_prodsys_p)
export_dataframe(eth_amr_prodsys_p, PRODATA_FOLDER)
export_dataframe(eth_amr_prodsys_p, DASHDATA_FOLDER)

# -----------------------------------------------------------------------------
#### -- Reshape for plotting
# -----------------------------------------------------------------------------
# Melt and merge relies on consistent column ordering
# Medians
eth_amr_prodsys_p_median = eth_amr_prodsys_p.melt(
	id_vars='production_system'
	,value_vars=['amr_production_losses_median_usd', 'ahle_withoutamr_mean_usd']
	,var_name='metric'
	,value_name='value_usd'
)

# Errors
eth_amr_prodsys_p_errhigh = eth_amr_prodsys_p.melt(
	id_vars='production_system'
	,value_vars=['amr_production_losses_errhigh_usd', 'ahle_withoutamr_errhigh_usd']
	,var_name='metric'
	,value_name='error_high_usd'
)
eth_amr_prodsys_p_errlow = eth_amr_prodsys_p.melt(
	id_vars='production_system'
	,value_vars=['amr_production_losses_errlow_usd', 'ahle_withoutamr_errlow_usd']
	,var_name='metric'
	,value_name='error_low_usd'
)

# Put them together
eth_amr_prodsys_p_melt = eth_amr_prodsys_p_median.copy()
eth_amr_prodsys_p_melt['error_high_usd'] = eth_amr_prodsys_p_errhigh['error_high_usd']
eth_amr_prodsys_p_melt['error_low_usd'] = eth_amr_prodsys_p_errlow['error_low_usd']

# Currency is in column name, drop from metric
eth_amr_prodsys_p_melt['metric'] = eth_amr_prodsys_p_melt['metric'].str.replace('_usd', '', regex=False)

# -----------------------------------------------------------------------------
#### -- Add exchange rate
# -----------------------------------------------------------------------------
eth_amr_prodsys_p_melt['value_birr'] = eth_amr_prodsys_p_melt['value_usd'] * birr_per_usd_2021
eth_amr_prodsys_p_melt['error_high_birr'] = eth_amr_prodsys_p_melt['error_high_usd'] * birr_per_usd_2021
eth_amr_prodsys_p_melt['error_low_birr'] = eth_amr_prodsys_p_melt['error_low_usd'] * birr_per_usd_2021

# -----------------------------------------------------------------------------
#### -- Export
# -----------------------------------------------------------------------------
datainfo(eth_amr_prodsys_p_melt)
export_dataframe(eth_amr_prodsys_p_melt, PRODATA_FOLDER)
export_dataframe(eth_amr_prodsys_p_melt, DASHDATA_FOLDER)

# -----------------------------------------------------------------------------
#### -- Population by Production System
# -----------------------------------------------------------------------------
eth_pop_prodsys_imp = pd.read_excel(
    os.path.join(RAWDATA_FOLDER, 'results_ethiopia_5March25_JREdit.xlsx')
    ,sheet_name='Population per prod syst'
)
eth_pop_prodsys_imp = clean_colnames(eth_pop_prodsys_imp)
datainfo(eth_pop_prodsys_imp)

# =============================================================================
#### Test plot 1
# =============================================================================
# import pandas as pd
# import numpy as np
# import plotly.graph_objects as go
# from plotly.subplots import make_subplots

# # Your data
# data = {
#     'Metric': [
#         'Production losses due to resistant mastitis (USD)',
#         'Expenditure with resistant mastitis (USD)',
#         'Indirect costs due to AMR (USD)',
#         'AHLE - cattle (USD)'
#     ],
#     'Mean': [
#         685208346.22,
#         136823.49,
#         279555.32,
#         15420000000.00
#     ],
#     'Lower_95_CI': [
#         522339460.51,
#         66345.88,
#         279555.32,
#         12700000000.00
#     ],
#     'Upper_95_CI': [
#         825158203.48,
#         219368.87,
#         279555.32,
#         18570000000.00
#     ]
# }

# # Convert to DataFrame
# df = pd.DataFrame(data)

# # Calculate error margins
# df['Error_Minus'] = df['Mean'] - df['Lower_95_CI']
# df['Error_Plus'] = df['Upper_95_CI'] - df['Mean']

# # Create shorter labels for display
# df['Short_Label'] = [
#     'Production losses',
#     'Expenditure',
#     'Indirect costs',
#     'AHLE - cattle'
# ]

# # Sort data by Mean value in descending order to have largest at bottom of stack
# df = df.sort_values('Mean')

# # Create figure
# fig = go.Figure()

# # Calculate cumulative sums for stacking
# df['Cumulative_Sum'] = df['Mean'].cumsum()
# df['Previous_Sum'] = df['Cumulative_Sum'].shift(1).fillna(0)

# # Add traces for each metric (stacked)
# colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']

# for i, row in df.iterrows():
#     fig.add_trace(go.Bar(
#         name=row['Short_Label'],
#         x=['AMR Economic Impact'],
#         y=[row['Mean']],
#         base=[row['Previous_Sum']],
#         marker_color=colors[i % len(colors)],
#         text=f"${row['Mean']:,.2f}",
#         textposition='inside',
#         insidetextanchor='middle',
#         error_y=dict(
#             type='data',
#             symmetric=False,
#             array=[row['Error_Plus']],
#             arrayminus=[row['Error_Minus']],
#             visible=True
#         ),
#         hovertemplate='<b>%{data.name}</b><br>Value: $%{y:,.2f}<br>Lower CI: $%{customdata[0]:,.2f}<br>Upper CI: $%{customdata[1]:,.2f}',
#         customdata=[[row['Lower_95_CI'], row['Upper_95_CI']]]
#     ))

# # Calculate total value
# total_value = df['Mean'].sum()

# # Update layout
# fig.update_layout(
#     title='Economic Impact of Antimicrobial Resistance (AMR)',
#     yaxis_title='USD ($)',
#     barmode='stack',
#     legend_title='Cost Categories',
#     template='plotly_white',
#     height=700,
#     width=900,
#     yaxis=dict(
#         type='log',  # Using log scale due to large differences in values
#         title='USD (Log Scale)'
#     ),
#     legend=dict(
#         orientation='h',
#         yanchor='bottom',
#         y=1.02,
#         xanchor='center',
#         x=0.5
#     )
# )

# # Add annotation for total value
# fig.add_annotation(
#     x='AMR Economic Impact',
#     y=total_value * 1.1,  # Slightly above the top of the bar
#     text=f'Total: ${total_value:,.2f}',
#     showarrow=False,
#     font=dict(size=14, color='black', family='Arial Black')
# )

# # Show the figure
# fig.show()

# # To save the figure
# # fig.write_html("amr_economic_impact_stacked_bar.html")

# =============================================================================
#### Test plot 2 - side by side stacked bars
# =============================================================================
# '''
# JR Note: even when giving AMR metrics their own Y-axis, indirect costs and
# health expenditure are invisible! Log axis needed.
# '''
# import plotly.graph_objs as go
# from plotly.subplots import make_subplots
# import plotly.io as pio

# # Data
# data = {
#     'metric': [
#         'AMR production losses',
#         'AMR health expenditure',
#         'AMR indirect costs',
#         'Unattributed AHLE'
#     ],
#     'value_usd': [
#         685208346.2217035,
#         136823.4928812638,
#         279555.3191489362,
#         14734375274.966267
#     ]
# }

# # Create figure with two subplots side by side
# fig = make_subplots(
#     rows=1,
#     cols=2,
#     subplot_titles=('Full View', 'Zoomed View (AMR Metrics)'),
#     # shared_legend=True,
#     x_title='View Type',
#     specs=[[{'type':'bar'}, {'type':'bar'}]]
# )

# # Full view bar chart (left)
# fig.add_trace(
#     go.Bar(
#         x=['Full View'],
#         y=[data['value_usd'][0]],
#         name='AMR production losses',
#         marker_color='blue',
#         hovertemplate='AMR production losses: $%{y:,.2f}<extra></extra>'
#     ),
#     row=1, col=1
# )
# fig.add_trace(
#     go.Bar(
#         x=['Full View'],
#         y=[data['value_usd'][1]],
#         name='AMR health expenditure',
#         marker_color='green',
#         hovertemplate='AMR health expenditure: $%{y:,.2f}<extra></extra>',
#         base=data['value_usd'][0]
#     ),
#     row=1, col=1
# )
# fig.add_trace(
#     go.Bar(
#         x=['Full View'],
#         y=[data['value_usd'][2]],
#         name='AMR indirect costs',
#         marker_color='red',
#         hovertemplate='AMR indirect costs: $%{y:,.2f}<extra></extra>',
#         base=data['value_usd'][0] + data['value_usd'][1]
#     ),
#     row=1, col=1
# )
# fig.add_trace(
#     go.Bar(
#         x=['Full View'],
#         y=[data['value_usd'][3]],
#         name='Unattributed AHLE',
#         marker_color='purple',
#         hovertemplate='Unattributed AHLE: $%{y:,.2f}<extra></extra>',
#         base=data['value_usd'][0] + data['value_usd'][1] + data['value_usd'][2]
#     ),
#     row=1, col=1
# )

# # Zoomed view bar chart (right) - only AMR metrics
# fig.add_trace(
#     go.Bar(
#         x=['Zoomed View'],
#         y=[data['value_usd'][0]],
#         name='AMR production losses',
#         marker_color='blue',
#         showlegend=False,
#         hovertemplate='AMR production losses: $%{y:,.2f}<extra></extra>'
#     ),
#     row=1, col=2
# )
# fig.add_trace(
#     go.Bar(
#         x=['Zoomed View'],
#         y=[data['value_usd'][1]],
#         name='AMR health expenditure',
#         marker_color='green',
#         showlegend=False,
#         hovertemplate='AMR health expenditure: $%{y:,.2f}<extra></extra>',
#         base=data['value_usd'][0]
#     ),
#     row=1, col=2
# )
# fig.add_trace(
#     go.Bar(
#         x=['Zoomed View'],
#         y=[data['value_usd'][2]],
#         name='AMR indirect costs',
#         marker_color='red',
#         showlegend=False,
#         hovertemplate='AMR indirect costs: $%{y:,.2f}<extra></extra>',
#         base=data['value_usd'][0] + data['value_usd'][1]
#     ),
#     row=1, col=2
# )

# # Update layout
# fig.update_layout(
#     title='AMR Costs: Full View and Zoomed View',
#     barmode='stack',
#     height=600,
#     width=1200,
#     legend_title='Metrics'
# )

# # Customize y-axes
# fig.update_yaxes(
#     title_text='Cost (USD)',
#     tickformat='.2s',  # Scientific notation with 2 significant digits
#     row=1, col=1
# )
# fig.update_yaxes(
#     title_text='AMR Metrics Cost (USD)',
#     tickformat='.2s',  # Scientific notation with 2 significant digits
#     range=[0, sum(data['value_usd'][:3])],  # Set y-axis range for zoomed view
#     row=1, col=2
# )

# # Show the plot
# pio.show(fig)
