.PHONY: run-main run-streamlit run-all install-deps validate

# Command to install Python dependencies
install-deps:
	pip install -r requirements.txt

# Command to run the main Python script
run-main: install-deps
	python main.py

# Command to run the Streamlit application
run-streamlit: install-deps
	streamlit run charts.py

# Command to run both main and Streamlit application
run-all: install-deps run-main run-streamlit