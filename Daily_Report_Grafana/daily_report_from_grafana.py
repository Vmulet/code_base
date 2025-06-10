import requests
from PIL import Image
from reportlab.pdfgen import canvas
import os

# Grafana details
GRAFANA_URL = "https://ocg-graf.dyndns.org:44103/"
API_KEY = ""

def get_dashboards():
    """Fetch the list of dashboards from Grafana"""
    headers = {"Authorization": f"Bearer {API_KEY}"}
    response = requests.get(f"{GRAFANA_URL}/api/search", headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        print("Error fetching dashboards:", response.text)
        return []

def get_dashboard_image(dashboard_uid, image_name, theme):
    """Use the Grafana Renderer plugin to get a PNG image of the dashboard"""
    height = 3820
    width = 2160
    render_url = f"{GRAFANA_URL}/render/d/{dashboard_uid}?theme={theme}&orgId=1&from=now-24h&to=now&timezone=browser&height={height}&width={width}&autofitpanels&kiosk"
    headers = {"Authorization": f"Bearer {API_KEY}"}
    response = requests.get(render_url, headers=headers)
    if response.status_code == 200:
        with open(image_name, "wb") as file:
            file.write(response.content)
        print("Dashboard image saved as {}".format(image_name))
    else:
        print("Error rendering dashboard:", response.text)

def convert_png_to_pdf(png_path, pdf_path):
    """Convert PNG to PDF"""
    image = Image.open(png_path)
    pdf_canvas = canvas.Canvas(pdf_path)
    page_width, page_height = pdf_canvas._pagesize
    image_width, image_height = image.size
    scale = min(page_width / image_width, page_height / image_height)
    scaled_width = image_width * scale
    scaled_height = image_height * scale
    pdf_canvas.drawInlineImage(image, 0, page_height - scaled_height, scaled_width, scaled_height)
    pdf_canvas.showPage()
    pdf_canvas.save()
    print(f"Converted {png_path} to {pdf_path}")

def main(pdf_name, theme = "light"):
    dashboards = get_dashboards()
    if dashboards:
        dashboard_uid = dashboards[0]["uid"] 
        dashboard_uid = "bejdt1c1gwq2ob/daily-report-dev"
        image_dashboard = pdf_name.replace(".pdf", ".png")
        get_dashboard_image(dashboard_uid, image_dashboard, theme)
        convert_png_to_pdf(image_dashboard, pdf_name)
        os.remove(image_dashboard)

main('2025-05-30_Daily_Report_OCG_DATA.pdf')