from pathlib import Path
import streamlit as st

st.markdown((Path(__file__).parent / "intro.md").read_text())
