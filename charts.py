import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

# Function to load data with caching
@st.cache_data
def load_data(file):
    df = pd.read_csv(file)
    return df

# Helper function to set up and render a plot
def setup_and_render_plot(df, plot_type, x_col=None, y_cols=None, **kwargs) -> None:
    fig, ax1 = plt.subplots(figsize=(10, 6))
    ax1.set_xlabel(kwargs.get('xlabel', ''))
    ax1.set_ylabel(kwargs.get('ylabel', ''))
    ax1.set_title(kwargs.get('title', ''))

    colors = ['tab:blue', 'tab:orange']
    
    if plot_type == 'line':
        if len(y_cols) == 1:
            ax1.plot(df[x_col], df[y_cols[0]], label=y_cols[0], marker='o', color=colors[0])
            for x, y in zip(df[x_col], df[y_cols[0]]):
                ax1.annotate(f'{y:.2f}', (x, y), textcoords="offset points", xytext=(0, 5), ha='center')
        elif len(y_cols) == 2:
            # Make a second axes
            ax2 = ax1.twinx()
            ax1.plot(df[x_col], df[y_cols[0]], label=y_cols[0], marker='o', color=colors[0])
            ax2.plot(df[x_col], df[y_cols[1]], label=y_cols[1], marker='o', color=colors[1])

            ax1.set_ylabel(f'{y_cols[0]} ({kwargs.get("aggregation")})', color=colors[0])
            ax2.set_ylabel(f'{y_cols[1]} ({kwargs.get("aggregation")})', color=colors[1])
            ax1.tick_params(axis='y', labelcolor=colors[0])
            ax2.tick_params(axis='y', labelcolor=colors[1])

            fig.tight_layout()

            fig.legend(loc='upper left', bbox_to_anchor=(0.1, 0.9))
            
            # Annotate all data points
            for y_col in y_cols:
                for x, y in zip(df[x_col], df[y_col]):
                    ax1.annotate(f'{y:.2f}', (x, y), textcoords="offset points", xytext=(0, 5), ha='right')
                    ax2.annotate(f'{y:.2f}', (x, y), textcoords="offset points", xytext=(0, 8), ha='left')
        else:
            for idx, y_col in enumerate(y_cols):
                ax1.plot(df[x_col], df[y_col], label=y_col, marker='o', color=colors[idx % len(colors)])
                ax2.plot(df[x_col], df[y_col], label=y_col, marker='o', color=colors[idx % len(colors)])
                
                # Annotate all data points
                for x, y in zip(df[x_col], df[y_col]):
                    ax1.annotate(f'{y:.2f}', (x, y), textcoords="offset points", xytext=(0, 5), ha='right')
                    ax2.annotate(f'{y:.2f}', (x, y), textcoords="offset points", xytext=(0, 8), ha='left')
            ax1.legend()
            ax2.legend()

    plt.tight_layout()
    st.pyplot(fig)

def plot_pdau_trends_over_weeks(df, cols, aggregation) -> None:
    if isinstance(cols, str):
        cols = [cols]

    df_grouped = df.groupby('login_week').agg({col: aggregation for col in cols}).reset_index()
    df_grouped.columns = ['Week'] + [f'{aggregation} {col}' for col in cols]

    setup_and_render_plot(
        df_grouped,
        'line',
        x_col='Week',
        y_cols=[f'{aggregation} {col}' for col in cols],
        xlabel='Week',
        ylabel='Value',
        title=f'Trends Over Weeks ({aggregation})',
        aggregation=aggregation,
        legend_label=[f'{aggregation} {col}' for col in cols]
    )

def show_charts() -> None:
    st.title("PDAU Analysis Dashboard")

    st.write("""
        This app shows how Paying Daily Active Users (PDAU) change over time based on the data collected.
        You can explore the trends of PDAU, user logins, and transaction value over the weeks 31 to 40 in 2020.
    """)

    df_pdau = load_data("data/gold/pdau_by_country_login_date.csv.gz")

    st.header("PDAU and Logins Over Weeks")
    plot_pdau_trends_over_weeks(df_pdau, ['pdau', 'total_logins'], 'mean')

    st.header("PDAU Percentage Over Weeks")
    plot_pdau_trends_over_weeks(df_pdau, ['pdau_percentage'], 'mean')

    st.header("Revenue Over Weeks")
    plot_pdau_trends_over_weeks(df_pdau, 'amount_eur', 'mean')

    with open("analysis.txt", 'r') as analysis_file:
        analysis_content = analysis_file.read()
        st.write(analysis_content)


show_charts()