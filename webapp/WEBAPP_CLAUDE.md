# Cars Dashboard Webapp - Technical Documentation

## General Instructions
- The default Dash run command is 'run'; not 'run-server'
- To start the app: `python webapp/app_new.py` (or `webapp/app.py` after migration)
- Default port: 8051

## Architecture Overview

### Modular Structure (Post-Refactoring)
The webapp uses a clean, modular architecture:

```
webapp/
├── app_new.py                 # Main entry point
├── components/                # UI layout components
│   ├── filters.py            # Filter sidebar
│   ├── results.py            # Results display
│   ├── chat.py               # AI chat interface
│   └── layouts.py            # Page layouts
├── callbacks/                 # Interactive callbacks
│   ├── filter_callbacks.py   # Filter controls
│   ├── results_callbacks.py  # Results updates
│   ├── chat_callbacks.py     # AI chat logic
│   └── routing_callbacks.py  # Page routing
└── utils/                     # Helpers and utilities
    ├── data_loader.py        # Data loading from Databricks
    ├── helpers.py            # Formatting functions
    └── styles.py             # CSS and styling
```

See `REFACTORING_GUIDE.md` for detailed architecture documentation.

## Dashboard Layout Structure

### Three-Column Layout
- **Left Sidebar (16.7% width, Bootstrap col-2)**: Filter components with scrollable overflow
- **Center Panel (66.7% width, Bootstrap col-8)**: Results display with scrollable overflow
- **Right Sidebar (16.7% width, Bootstrap col-2)**: AI Chat interface with scrollable overflow
- **Header Row**: Full-width gradient header with app title "Cars Dashboard by Cauchy.io"

### View Modes
- **List View** (default): Horizontal cards with thumbnail on left
- **Grid View**: 3-column grid of compact cards
- Toggle buttons in results header

## Filter Components

### Available Filters

| Filter | Type | Default | Notes |
|--------|------|---------|-------|
| **Brand (Merk)** | Single-select dropdown | First brand | Updates model dropdown dynamically |
| **Model (Handelsbenaming)** | Multi-select dropdown | Empty | Populated based on selected brand |
| **Vehicle Type (Voertuigsoort)** | Multi-select dropdown | First type | e.g., Personenauto |
| **Primary Color (Eerste Kleur)** | Multi-select dropdown | Empty | Exclude 'Niet geregistreerd' |
| **Engine Size (Cilinderinhoud)** | Range slider | 500-8000cc | Step: 100cc |
| **Vehicle Weight (Massa Ledig)** | Range slider | 500-5000kg | Step: 50kg |
| **Number of Seats (Aantal Zitplaatsen)** | Range slider | 1-7+ | Step: 1, 7 means "7 or more" |
| **Registration Year** | Range slider | 2000-2020 | Based on datum_eerste_toelating |

### Filter Behavior
- **Live Updates**: Filters apply immediately without "Apply" button
- **Active Filter Count**: Badge shows number of active filters
- **Clear All**: Button to reset all filters to defaults
- **Smart Defaults**: Range sliders only filter when changed from defaults
- **Dynamic Dependencies**: Model dropdown updates when brand changes

## Results Display Format

### Card Layout

#### List View (Default)
- Horizontal card layout with 2-column grid (thumbnail + info)
- **Thumbnail**: 70x70px placeholder with car icon
- **Car Name**: Brand + Model as H5 heading
- **Description**: Key specs separated by bullet points
- **License Plate**: Formatted kenteken in green
- **Hover Effect**: Card lifts and changes border color

#### Grid View
- 3-column responsive grid (4 cols each in Bootstrap)
- Compact vertical cards (200px height)
- Centered icon, name, specs, and kenteken
- Same hover effects as list view

### Result Information
- Shows total matches and displayed count
- Limited to 25 entries maximum for performance
- Info banner: "Found X matches, showing Y results"
- Empty states for no results or no filters applied

### Car Detail Page
- Accessible by clicking any car card
- URL format: `/car/{kenteken}`
- Organized into categorized sections:
  - General (brand, model, type, color, seats, doors)
  - Historic (registration dates, APK, etc.)
  - Weights/Dimensions (mass, dimensions)
  - Engine/Emissions (engine specs, fuel, CO2)
  - Technical (wheels, speed, certifications)
- Each category has color-coded cards with icons
- Back button to return to main dashboard

## Data Structure

### Data Source
- **Primary Source**: Databricks gold table `gold_catalog.rdw_etl.gekentekende_voertuigen`
- **Filter**: Only "Personenauto" vehicle types loaded
- **In-Memory Storage**: Loaded once at startup into Polars DataFrame
- **Performance**: Full dataset in memory for fast filtering

### Key Columns
| Column | Description | Usage |
|--------|-------------|-------|
| `kenteken` | License plate number | Primary identifier, displayed formatted |
| `merk` | Brand/make | Brand filter, car name |
| `handelsbenaming` | Model name | Model filter, car name |
| `voertuigsoort` | Vehicle type | Vehicle type filter |
| `eerste_kleur` | Primary color | Color filter |
| `cilinderinhoud` | Engine size (cc) | Engine size filter, display |
| `massa_ledig_voertuig` | Curb weight (kg) | Weight filter, display |
| `aantal_zitplaatsen` | Number of seats | Seats filter, display |
| `datum_eerste_toelating` | First registration date (YYYYMMDD) | Year filter, display |

### Data Exclusions
- 'Niet geregistreerd' values excluded from all dropdowns
- Empty/null values excluded from unique value lists
- Only valid data used for filter population

## AI Chat System

### Architecture
- **Class**: `AIChat` in `webapp/ai_chat.py`
- **Provider**: Hugging Face InferenceClient with Novita provider
- **Model**: DeepSeek-V3-0324 for natural language processing
- **Authentication**: Requires `HF_TOKEN` environment variable

### Response Structure
The AI returns structured JSON with two components:

```json
{
  "message": "Natural language response to user",
  "filters": {
    "brands": ["BMW", "Mercedes"],
    "models": null,
    "vehicle_types": null,
    "colors": ["ZWART", "WIT"],
    "engine_range": [1500, 3000],
    "weight_range": null,
    "seats": [4, 7],
    "year_range": [2015, 2020]
  }
}
```

### Filter Categories
| Category | Type | Example | Notes |
|----------|------|---------|-------|
| `brands` | List[str] | `["BMW", "Mercedes"]` | Brand names, single selected on apply |
| `models` | List[str] | `["320i", "520d"]` | Model names for selected brand |
| `vehicle_types` | List[str] | `["Personenauto"]` | Vehicle types |
| `colors` | List[str] | `["ZWART", "WIT"]` | Color names (uppercase) |
| `engine_range` | [int, int] | `[1500, 3000]` | Min/max engine size in cc |
| `weight_range` | [int, int] | `[1000, 2000]` | Min/max weight in kg |
| `seats` | [int, int] | `[4, 7]` | Min/max seats (7 = 7+) |
| `year_range` | [int, int] | `[2015, 2020]` | Min/max registration year |

### Chat Behavior
- **Filter Merging**: AI suggestions merged with current filter state
- **Null Values**: `null` in suggestions means "keep current value"
- **Change Detection**: UI shows which filters were updated
- **Chat History**: Maintained in component state
- **Visual Feedback**: User messages right-aligned, AI responses left-aligned with robot icon

### Error Handling
- Graceful fallback if AI API fails
- JSON parsing errors return user-friendly messages
- Current filter state maintained on errors
- No crashes from AI failures

## Styling & UX

### Color Scheme
- **Primary Gradient**: `#667eea` → `#4285f4` (purple to blue)
- **Accent Colors**: Category-specific (see `utils/styles.py`)
- **Background**: `#f8f9fc` (light gray-blue)
- **Cards**: White with subtle shadows

### Interactions
- **Hover Effects**: Cards lift and change border color
- **Smooth Transitions**: All state changes animated
- **Loading Indicators**: Shown during data operations
- **Responsive Design**: Works on different screen sizes

### Typography
- **Font Family**: 'Inter', 'Segoe UI', 'Roboto', sans-serif
- **Headings**: Bold with gradient underlines
- **Body Text**: 14px for readability
- **Icons**: Font Awesome 6.4.0

## Performance Considerations

### Data Loading
- **Startup**: Data loaded once into memory (Polars DataFrame)
- **Fast Filtering**: All operations on in-memory data
- **Result Limiting**: Maximum 25 results displayed
- **Lazy Conversion**: Only displayed rows converted to Pandas

### Optimization Strategies
- **Polars over Pandas**: Faster filtering operations
- **Single Data Load**: No repeated database queries
- **Minimal Conversions**: Only convert subset for display
- **Smart Defaults**: Avoid unnecessary filtering operations

## Testing Guidelines

### Component Testing
```python
# Test layout functions
from webapp.components import create_filters_sidebar
layout = create_filters_sidebar(["BMW", "Audi"], ["Personenauto"], ["ZWART"])
assert layout is not None
```

### Helper Testing
```python
# Test formatting functions
from webapp.utils import format_kenteken
assert format_kenteken("ABCD12") == "AB-CD-12"
assert format_kenteken("12ABC3") == "12-AB-C3"
```

### Data Loading Testing
```python
# Test data loading
from webapp.utils import load_car_data
df, brands, types, colors = load_car_data()
assert len(df) > 0
assert len(brands) > 0
```

### AI Chat Testing
- Test response structure (message + filters dict)
- Test all filter categories present in response
- Test filter value types match specification
- Test error handling maintains structure consistency
- Test various user inputs for robustness

## Common Tasks

### Adding a New Filter
1. Add UI component to `components/filters.py`
2. Add callback logic to `callbacks/filter_callbacks.py`
3. Update filtering logic in `callbacks/results_callbacks.py`
4. Update AI prompt in `ai_chat.py` to recognize new filter
5. Test end-to-end flow

### Modifying Layout
1. Update relevant component in `components/`
2. Adjust styling in `utils/styles.py` if needed
3. Test responsive behavior

### Debugging
- Check browser console for JavaScript errors
- Check terminal for Python exceptions
- Verify Databricks connection if data issues
- Test with `debug=True` in `app.run()`

## Environment Setup

### Required Environment Variables
```bash
HF_TOKEN=your_huggingface_token  # For AI chat functionality
```

### Dependencies
```bash
# Core dependencies
dash
dash-bootstrap-components
polars
pandas
databricks-connect  # or databricks-sdk
```

## Future Enhancements

### Planned Features
- [ ] Pagination for large result sets
- [ ] Export results to CSV
- [ ] Save/load filter presets
- [ ] User favorites/bookmarks
- [ ] Advanced search with boolean operators
- [ ] Comparison view for multiple cars

### Performance Improvements
- [ ] Implement virtual scrolling for large lists
- [ ] Add caching for AI responses
- [ ] Optimize Polars operations further
- [ ] Consider database-side filtering for very large datasets

## References

- **Architecture Details**: See `REFACTORING_GUIDE.md`
- **Project Guidelines**: See root `CLAUDE.md`
- **AI Chat Implementation**: See `webapp/ai_chat.py`
- **Component API**: See docstrings in `webapp/components/`