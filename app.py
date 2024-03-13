from flask import Flask, request, jsonify
from src.logger import logging
import sys

app = Flask(__name__)

@app.route("/input", methods=["POST"])
def make_url():
    try:
        data = request.get_json()
        job_Keyword = data["job_Keyword"]
        Location_Keyword = data["Location_Keyword"]
        Filter_option = int(data["Filter_option"])  

        user_details = {
            "job_Keyword": job_Keyword,
            "Location_Keyword": Location_Keyword,
            "Filter_option": Filter_option,
        }

        default_url = {
            1: "https://www.linkedin.com/jobs/search/?keywords={}&location={}&origin=JOB_SEARCH_PAGE_LOCATION_HISTORY&refresh=true",
            2: "https://www.linkedin.com/jobs/search/?f_TPR=r2592000&keywords={}&location={}&origin=JOB_SEARCH_PAGE_JOB_FILTER&refresh=true",
            3: "https://www.linkedin.com/jobs/search/?f_TPR=r604800&keywords={}&location={}&origin=JOB_SEARCH_PAGE_JOB_FILTER&refresh=true",
            4: "https://www.linkedin.com/jobs/search/?f_TPR=r86400&keywords={}&location={}&origin=JOB_SEARCH_PAGE_JOB_FILTER&refresh=true",
        }
        
        if 1 <= Filter_option <= 4:
            default_selected_filter = default_url[Filter_option]
            logging.info(f"user selected default url collected - {user_details}")
            logging.info(f"user selected default url collected - {default_selected_filter}")
            final_url = default_selected_filter.format(job_Keyword, Location_Keyword)
            return jsonify({
                "message": "User details and Links collected successfully",
                "code": 200,
                "final_url": final_url
            })

        else:
            return jsonify({
                "message": "Invalid Filter_option. Filter_option should be between 1 and 4.",
                "status": "error"
            }), 400

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return jsonify({
            "message": "Internal server error",
            "status": "error",
            "error": str(e),
        }), 500

if __name__ == "__main__":
    app.run()
