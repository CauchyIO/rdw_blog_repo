"""CSS styles and styling constants for the webapp."""

# Custom CSS to be injected into the app
CUSTOM_CSS = """
            body, html {
                overflow-x: hidden !important;
                max-width: 100vw !important;
            }

            .car-card:hover {
                transform: translateY(-2px);
                box-shadow: 0 8px 20px rgba(0,0,0,0.12) !important;
                border-color: #E05842 !important;
            }

            a:has(.car-card) {
                text-decoration: none !important;
                color: inherit !important;
            }

            a:has(.car-card):hover {
                text-decoration: none !important;
                color: inherit !important;
            }

            .license-plate-container {
                position: relative;
                display: inline-block;
                margin: 0 auto;
            }

            .eu-badge {
                position: absolute;
                left: 5px;
                top: 50%;
                transform: translateY(-50%);
                width: 48px;
                height: 82px;
                background: #003399;
                color: white;
                display: flex;
                align-items: center;
                justify-content: center;
                font-weight: bold;
                font-size: 18px;
                border-radius: 4px 0 0 4px;
                z-index: 1;
            }

            .license-plate-input {
                background: linear-gradient(180deg, rgb(247, 207, 71) 0%, rgb(248, 216, 103) 50%, rgb(206, 161, 3) 100%) !important;
                border: 3px solid #000 !important;
                border-radius: 8px !important;
                padding: 15px 20px 15px 70px !important;
                font-size: 48px !important;
                font-weight: 900 !important;
                letter-spacing: 8px !important;
                text-align: center !important;
                text-transform: uppercase !important;
                color: #000 !important;
                font-family: 'Arial Black', sans-serif !important;
                box-shadow: 0 4px 8px rgba(0,0,0, 0.3), inset 0 2px 4px rgba(255, 255, 255, 0.3) !important;
                width: 436px !important;
                height: 100px !important;
            }

            .nav-tabs .nav-link {
                border-radius: 8px 8px 0 0 !important;
                padding: 12px 24px !important;
                font-weight: 500 !important;
                color: #ffffff !important;
                border: 1px solid transparent !important;
                margin-right: 4px !important;
            }

            .nav-tabs .nav-link:hover {
                border-color: #e3e8ee !important;
                background-color: #f8f9fc !important;
                color: #636e72 !important;
            }

            .nav-tabs .nav-link.active {
                color: #E05842 !important;
                background-color: #ffffff !important;
                border-color: #e3e8ee #e3e8ee #ffffff !important;
            }

            .Select-control {
                border-radius: 8px !important;
                border-color: #e3e8ee !important;
            }

            .Select-control:hover {
                border-color: #E05842 !important;
            }

            .rc-slider-track {
                background: linear-gradient(90deg, #E05842 0%, #C74430 100%) !important;
            }

            .rc-slider-handle {
                border-color: #E05842 !important;
                background: #E05842 !important;
            }

            .rc-slider-handle:hover {
                border-color: #C74430 !important;
                background: #C74430 !important;
            }

            ::-webkit-scrollbar {
                width: 8px;
            }

            ::-webkit-scrollbar-track {
                background: #f1f3f4;
                border-radius: 4px;
            }

            ::-webkit-scrollbar-thumb {
                background: linear-gradient(180deg, #E05842 0%, #C74430 100%);
                border-radius: 4px;
            }

            ::-webkit-scrollbar-thumb:hover {
                background: linear-gradient(180deg, #C74430 0%, #B83C28 100%);
            }
"""

# Index string template with custom styles
INDEX_STRING = f"""
<!DOCTYPE html>
<html>
    <head>
        {{%metas%}}
        <title>{{%title%}}</title>
        {{%favicon%}}
        {{%css%}}
        <style>
{CUSTOM_CSS}
        </style>
    </head>
    <body>
        {{%app_entry%}}
        <footer>
            {{%config%}}
            {{%scripts%}}
            {{%renderer%}}
        </footer>
    </body>
</html>
"""

# Category styling for car detail pages
CATEGORY_STYLES = {
    "General": ("info-circle", "#E05842"),
    "Historic": ("history", "#fd79a8"),
    "Weights/Dimensions": ("weight-hanging", "#2196f3"),
    "Engine/Emissions": ("cogs", "#e17055"),
    "Technical": ("tools", "#00cec9"),
    "External Data": ("external-link-alt", "#00b894"),
    "Other": ("question-circle", "#636e72"),
}


def get_custom_css() -> str:
    """Return the custom CSS index string for the Dash app."""
    return INDEX_STRING