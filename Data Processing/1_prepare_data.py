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
DASHDATA_FOLDER = os.path.join(PARENT_FOLDER, 'Dash App', 'data')

#%% DENMARK INITIAL PREVIEW DATA
# *****************************************************************************
# =============================================================================
#### Farm summary from UoL
# =============================================================================
den_farmsmry = pd.read_excel(
    os.path.join(RAWDATA_FOLDER, 'Denmark AMR data organizer JR.xlsx')
    ,sheet_name='Farm summary'
)
datainfo(den_farmsmry)
export_dataframe(den_farmsmry, PRODATA_FOLDER)

# =============================================================================
#### AMR from UoL
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
#### AHLE from UoL
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
#### AMR and AHLE combo from UoL
# =============================================================================
den_amr_ahle = pd.merge(
    left=den_amr
    ,right=den_ahle.drop(columns='number_of_farms')
    ,on='farm_type'
    ,how='left'
)

# Add calcs
#!!! When calculating AMR as a proportion of AHLE for the 5th and 95th percentiles, Joao's
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
# Reshape for plotting - farm level
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
# Reshape for plotting - population level
# -----------------------------------------------------------------------------
# Median estimates
den_amr_ahle_poplvl = den_amr_ahle.melt(
	id_vars=['scenario', 'farm_type', 'number_of_farms']
    #!!! Ordering matters for plotting with log scale. Want smaller value first (AMR).
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

# -----------------------------------------------------------------------------
# Test plot
# -----------------------------------------------------------------------------
# set_log_y = True

# # Bar with melted data same as Dash
# #!!! This is the only way to show separate error bars for AMR and AHLE
# input_df = den_amr_ahle_poplvl.query("scenario == 'Average'").query("farm_type != 'Total'")
# barchart_fig = px.bar(
#     input_df
#     ,x='farm_type'
#     ,y='value'
#     ,color='metric'
#     ,barmode='relative'
#     ,error_y='error_high'
#     ,error_y_minus='error_low'
#     ,log_y=set_log_y
#     )
# barchart_fig.update_layout(
#     title_text=f'Population-level AHLE and the Burden of Antimicrobial Resistance (AMR)<br>by Farm Type'
#     ,font_size=15
#     ,xaxis_title='Farm Type'
# 	,yaxis_title='Burden (DKK)'
# 	,legend_title_text='Source of Burden'
#     )
# barchart_fig.show()

# # Bar with unmelted data
# input_df = den_amr_ahle.query("scenario == 'Average'").query("farm_type != 'Total'")
# barchart_fig = px.bar(
#     input_df
#     ,x='farm_type'
#     ,y=['burden_of_amr_at_pop_level_median', 'ahle_at_pop_level_median_withoutamr']
#     ,barmode='relative'
#     ,log_y=set_log_y
#     ,error_y='burden_of_amr_at_pop_level_errhigh'
#     ,error_y_minus='burden_of_amr_at_pop_level_errlow'
#     )
# barchart_fig.update_layout(
#     title_text=f'Population-level AHLE and the Burden of Antimicrobial Resistance (AMR)<br>by Farm Type'
#     ,font_size=15
#     ,xaxis_title='Farm Type'
# 	,yaxis_title='Burden (DKK)'
# 	,legend_title_text='Source of Burden'
#     )
# barchart_fig.show()

# # Histogram with melted data same as Dash
# input_df = den_amr_ahle_poplvl.query("scenario == 'Average'").query("farm_type != 'Total'")
# barchart_fig = px.histogram(
#     input_df
#     ,x='farm_type'
#     ,y='value'
#     ,color='metric'
#     ,log_y=set_log_y
#     ,barnorm='percent'
#     ,text_auto='.1f'
#     )
# barchart_fig.update_layout(
#     title_text=f'Population-level AHLE and the Burden of Antimicrobial Resistance (AMR)<br>by Farm Type'
#     ,font_size=15
#     ,xaxis_title='Farm Type'
# 	,yaxis_title='% of AHLE'
# 	,legend_title_text='Source of Burden'
#     )
# barchart_fig.show()

# # Histogram with unmelted data
# input_df = den_amr_ahle.query("scenario == 'Average'").query("farm_type != 'Total'")
# barchart_fig = px.histogram(
#     input_df
#     ,x='farm_type'
#     ,y=['burden_of_amr_at_pop_level_median', 'ahle_at_pop_level_median_withoutamr']
#     ,log_y=set_log_y
#     ,barnorm='percent'
#     ,text_auto='.1f'
#     )
# barchart_fig.update_layout(
#     title_text=f'Population-level AHLE and the Burden of Antimicrobial Resistance (AMR)<br>by Farm Type'
#     ,font_size=15
#     ,xaxis_title='Farm Type'
# 	,yaxis_title='% of AHLE'
# 	,legend_title_text='Source of Burden'
#     )
# barchart_fig.show()

# =============================================================================
#### Fixing hidden error bars for stacked bar chart
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
'''
# -----------------------------------------------------------------------------
# Results from expert opinion method
# -----------------------------------------------------------------------------
den_amr_final_eo = pd.read_excel(
    os.path.join(RAWDATA_FOLDER, 'Results Denmark.xlsx')
    ,sheet_name='Expert opinion'
    ,header=[1, 2]
)

# Cleanup column names
den_amr_final_eo = colnames_from_index(den_amr_final_eo)
den_amr_final_eo = clean_colnames(den_amr_final_eo)
rename_cols = {
    "item_unnamed:_0_level_1":"item"
    ,"scenario_unnamed:_1_level_1":"scenario"
}
den_amr_final_eo = den_amr_final_eo.rename(columns=rename_cols)

# Separate AHLE and expenditure numbers
_row_select = (den_amr_final_eo['item'].str.upper().isin(['AHLE' ,'EXPENDITURE DKK']))
den_amr_final_eo_ahle = den_amr_final_eo.loc[_row_select]
den_amr_final_eo_amr = den_amr_final_eo.loc[~ _row_select]

# Transpose item names
den_amr_final_eo_amr_p = den_amr_final_eo_amr.pivot(
	index=['scenario']       # Column(s) to make new index. If blank, uses existing index.
	,columns=['item']        # Column(s) whose values define new columns
)
den_amr_final_eo_amr_p = colnames_from_index(den_amr_final_eo_amr_p)
den_amr_final_eo_amr_p = den_amr_final_eo_amr_p.reset_index()
den_amr_final_eo_amr_p = clean_colnames(den_amr_final_eo_amr_p)

datainfo(den_amr_final_eo_amr_p)

# -----------------------------------------------------------------------------
# Results from internal method
# -----------------------------------------------------------------------------
den_amr_final = pd.read_excel(
    os.path.join(RAWDATA_FOLDER, 'Results Denmark.xlsx')
    ,sheet_name='Our method'
    ,header=[1, 2]   # Header is split over two rows
)

#%% PLOTLY CODE FROM CLAUDE
# *****************************************************************************
# Create a DataFrame with the sales and error data
df = pd.DataFrame({
    'Quarter': ['Q1 2023', 'Q2 2023', 'Q3 2023', 'Q4 2023'] * 3,
    'Product': np.repeat(['Electronics', 'Clothing', 'Home Goods'], 4),
    'Sales': [
        # Electronics
        150, 180, 200, 220,
        # Clothing
        200, 220, 240, 260,
        # Home Goods
        100, 120, 150, 180
    ],
    'Error': [
        # Electronics errors
        15, 18, 20, 22,
        # Clothing errors
        20, 22, 24, 26,
        # Home Goods errors
        10, 12, 15, 18
    ]
})

# Calculate cumulative sales for each product
df = df.sort_values(['Quarter', 'Product']).reset_index(drop=True)
df['Cumulative_Sales_Over_Products'] = df.groupby('Quarter')['Sales'].cumsum()

# Create traces for each product line
traces = []
product_lines = df['Product'].unique()

# # Group the data to create stacked bars
# grouped = df.groupby(['Quarter', 'Product'])['Sales'].first().unstack()
# error_grouped = df.groupby(['Quarter', 'Product'])['Error'].first().unstack()

# for product in product_lines:
#     traces.append(go.Bar(
#         name=product,
#         x=grouped.index,
#         y=grouped[product],
#         error_y=dict(
#             type='data',
#             array=error_grouped[product],
#             arrayminus=error_grouped[product]*0.5,
#             visible=True,
#             color='black',
#             thickness=2,
#             width=5,
#         ),
#         marker_color={'Electronics': 'blue', 'Clothing': 'green', 'Home Goods': 'red'}[product]
#     ))

## JR: modifying code to use ungrouped data, draw error bars last
for i, product in enumerate(product_lines):
    traces.append(go.Bar(
        name=product,
        x=df.query(f"Product == '{product}'")['Quarter'],
        y=df.query(f"Product == '{product}'")['Sales'],
        # error_y=dict(
        #     type='data',
        #     array=df.query(f"Product == '{product}'")['Error'],
        #     visible=True,
        #     color='gray',
        #     thickness=2,
        #     width=5
        # ),
        marker_color=['blue', 'green', 'red'][i]  # Different color for each product line
    ))

## Add error bars
for i, product in enumerate(product_lines):
    traces.append(go.Scatter(
        name=product,
        x=df.query(f"Product == '{product}'")['Quarter'],
        y=df.query(f"Product == '{product}'")['Cumulative_Sales_Over_Products'],
        mode='markers',
        marker=dict(color='gray'),
        error_y=dict(
            type='data',
            array=df.query(f"Product == '{product}'")['Error'],
            # arrayminus=,      # Use for asymmetric errors
            visible=True,
            color='gray',
            thickness=2,
            width=5
        ),
        showlegend=False,
    ))

# Create the layout
layout = go.Layout(
    title='Quarterly Sales by Product Line',
    barmode='stack',
    xaxis={'title': 'Quarter'},
    yaxis={'title': 'Sales Volume'},
    legend_title='Product Categories',
    template='plotly_white',
    height=600,
    width=800
)

# Create figure and show
fig = go.Figure(data=traces, layout=layout)
fig.show()

# Optional: Print the DataFrame to show its structure
# print(df)
# print("\nSales by Quarter and Product:")
# print(grouped)
# print("\nError by Quarter and Product:")
# print(error_grouped)
