"""Cinema 360 Dashboard - Main Entry Point.

Streamlit application for movie analytics visualization.
"""
import streamlit as st

# Page configuration
st.set_page_config(
    page_title="Cinema 360 Analytics",
    page_icon="C360",
    layout="wide",
    initial_sidebar_state="expanded",
)


def main() -> None:
    """Main dashboard entry point."""
    st.title("Cinema 360 - Data Intelligence Platform")
    st.markdown("---")

    st.markdown("""
    ## Welcome to Cinema 360 Analytics Dashboard

    This dashboard provides insights into movie analytics including:
    - **Budget vs Revenue** correlation analysis
    - **Genre trends** over time
    - **Top performing movies** rankings

    Use the sidebar to navigate between different analysis views.
    """)

    # Sidebar
    st.sidebar.title("Navigation")
    st.sidebar.info(
        "Select a page from the sidebar to view different analytics."
    )


if __name__ == "__main__":
    main()
