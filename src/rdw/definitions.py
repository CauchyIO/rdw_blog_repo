from rdw.utils.models import RDWTable, BaseColumn
from pyspark.sql.types import StringType, IntegerType, DateType
from typing import Optional

rdw_tables = [
    RDWTable(
        url="https://opendata.rdw.nl/api/views/jqs4-4kvw/rows.csv?accessType=DOWNLOAD",
        name = "odometer_reading_judgment_explanation",
        description="Explanation of mileage reading judgments. Contains codes and detailed descriptions for odometer assessments based on registration history.",
        columns=[
            # odometer_judgment_explanation_code -> string because of entry "NG", all other entries are int (0-7)
            BaseColumn(input_col="Code toelichting tellerstandoordeel", output_col="odometer_judgment_explanation_code", original_type=int, output_data_type=StringType(), is_primary_key=True, is_nullable=False, description="Code that represents the judgment; also used as key for the detailed explanation."),
            BaseColumn(input_col="Toelichting tellerstandoordeel", output_col="odometer_judgment_explanation", original_type=str, output_data_type=StringType(), description="Explanation of the judgment that is given. The sequence arising after the last registration determines the judgment shown.")
        ]
    ),

    RDWTable(
        url="https://opendata.rdw.nl/api/views/m9d7-ebf2/rows.csv?accessType=DOWNLOAD",
        name = "registered_vehicles",
        description="Main dataset of registered vehicles in the Netherlands. Contains comprehensive vehicle information including registration details, technical specifications, ownership, and inspection data.",
        columns=[
            BaseColumn(input_col="Kenteken", output_col="license_plate", original_type=str, output_data_type=StringType(), is_primary_key=True, is_nullable=False, description="The license plate of a vehicle, consisting of a combination of numbers and letters. This makes the vehicle unique and identifiable."),
            BaseColumn(input_col="Voertuigsoort", output_col="vehicle_type", original_type=str, output_data_type=StringType(), description="European vehicle category, followed by the national vehicle type as registered in the license register. The national type is derived from the European category."),
            BaseColumn(input_col="Merk", output_col="make", original_type=str, output_data_type=StringType(), description="The make of the vehicle as specified by the manufacturer."),
            BaseColumn(input_col="Handelsbenaming", output_col="trade_name", original_type=str, output_data_type=StringType(), description="Trade name of the vehicle as provided by the manufacturer. May differ from what appears on the vehicle."),
            BaseColumn(input_col="Vervaldatum APK", output_col="mot_expiry_date", original_type=str, output_data_type=DateType(), description="The expiry date of the vehicle's periodic technical inspection (APK)."),
            BaseColumn(input_col="Datum tenaamstelling", output_col="registration_date", original_type=str, output_data_type=DateType(), description="The date on which the most recent owner was registered in the license register."),
            BaseColumn(input_col="Bruto BPM", output_col="gross_bpm", original_type=int, output_data_type=IntegerType(), description="Tax for passenger cars and motorcycles, paid once by the first registrant of the vehicle."),
            BaseColumn(input_col="Inrichting", output_col="interior_layout", original_type=str, output_data_type=StringType(), description="Description of the configuration or layout of the vehicle's interior."),
            BaseColumn(input_col="Aantal zitplaatsen", output_col="number_of_seats", original_type=int, output_data_type=IntegerType(), description="Maximum number of seats that are technically suitable to be used while driving."),
            BaseColumn(input_col="Eerste kleur", output_col="primary_color", original_type=str, output_data_type=StringType(), description="Primary color of the vehicle as registered. The RDW registers only main colors."),
            BaseColumn(input_col="Tweede kleur", output_col="secondary_color", original_type=str, output_data_type=StringType(), description="Secondary color of the vehicle, if applicable."),
            BaseColumn(input_col="Aantal cilinders", output_col="number_of_cylinders", original_type=int, output_data_type=IntegerType(), description="Number of cylinders in the vehicle's engine. Not applicable for electric or rotary engines."),
            BaseColumn(input_col="Cilinderinhoud", output_col="engine_displacement", original_type=int, output_data_type=IntegerType(), description="Total volume of the combustion chambers of the engine, expressed in cm³."),
            BaseColumn(input_col="Massa ledig voertuig", output_col="unladen_mass", original_type=int, output_data_type=IntegerType(), description="The unladen mass of the vehicle, excluding passengers and cargo, in kilograms."),
            BaseColumn(input_col="Toegestane maximum massa voertuig", output_col="permitted_maximum_mass", original_type=int, output_data_type=IntegerType(), description="The legally permitted maximum mass of the vehicle, potentially reduced by legal constraints or upon request."),
            BaseColumn(input_col="Massa rijklaar", output_col="kerb_weight", original_type=int, output_data_type=IntegerType(), description="Vehicle mass in ready-to-drive condition with 90% fuel and a 75 kg driver."),
            BaseColumn(input_col="Maximum massa trekken ongeremd", output_col="max_towing_mass_unbraked", original_type=int, output_data_type=IntegerType(), description="Maximum allowed mass in kg of an unbraked trailer the vehicle may tow."),
            BaseColumn(input_col="Maximum trekken massa geremd", output_col="max_towing_mass_braked", original_type=int, output_data_type=IntegerType(), description="Maximum allowed mass in kg of a braked trailer the vehicle may tow."),
            BaseColumn(input_col="Datum eerste toelating", output_col="first_admission_date", original_type=str, output_data_type=DateType(), description="Date when the vehicle was first registered (anywhere in the world)."),
            BaseColumn(input_col="Datum eerste tenaamstelling in Nederland", output_col="first_registration_date_nl", original_type=str, output_data_type=DateType(), description="Date when the vehicle was first registered in the Netherlands."),
            BaseColumn(input_col="Wacht op keuren", output_col="awaiting_inspection", original_type=str, output_data_type=StringType(), description="Indicates whether the vehicle must undergo an inspection due to damage or non-compliance."),
            BaseColumn(input_col="Catalogusprijs", output_col="catalog_price", original_type=int, output_data_type=IntegerType(), description="The list price as provided by the importer, including VAT and BPM. Mandatory for vehicles registered after July 1, 2005."),
            BaseColumn(input_col="WAM verzekerd", output_col="liability_insured", original_type=str, output_data_type=StringType(), description="Indicates whether the vehicle is registered as insured under the Dutch Motor Vehicle Insurance Liability Act."),
            BaseColumn(input_col="Maximale constructiesnelheid", output_col="max_design_speed", original_type=int, output_data_type=IntegerType(), description="Maximum speed determined by the design and construction of the vehicle, in km/h."),
            BaseColumn(input_col="Laadvermogen", output_col="payload_capacity", original_type=int, output_data_type=IntegerType(), description="Maximum allowed load weight for vans and trailers, in kilograms."),
            BaseColumn(input_col="Oplegger geremd", output_col="semi_trailer_braked", original_type=int, output_data_type=IntegerType(), description="Maximum permitted mass in kg of a braked semi-trailer that can be towed by the vehicle."),
            BaseColumn(input_col="Aanhangwagen autonoom geremd", output_col="trailer_autonomous_braked", original_type=int, output_data_type=IntegerType(), description="Maximum permitted mass in kg of a braked trailer with its own braking system that can be towed by the vehicle."),
            BaseColumn(input_col="Aanhangwagen middenas geremd", output_col="trailer_center_axle_braked", original_type=int, output_data_type=IntegerType(), description="Maximum permitted mass in kg of a braked trailer with a central axle that can be towed by the vehicle."),
            BaseColumn(input_col="Aantal staanplaatsen", output_col="number_of_standing_places", original_type=int, output_data_type=IntegerType(), description="Number of standing places that are technically suitable for use while driving."),
            BaseColumn(input_col="Aantal deuren", output_col="number_of_doors", original_type=int, output_data_type=IntegerType(), description="Number of doors intended for entry and exit of persons or cargo."),
            BaseColumn(input_col="Aantal wielen", output_col="number_of_wheels", original_type=int, output_data_type=IntegerType(), description="Total number of wheels mounted on the vehicle."),
            BaseColumn(input_col="Afstand hart koppeling tot achterzijde voertuig", output_col="coupling_center_to_rear_distance", original_type=int, output_data_type=IntegerType(), description="Distance in millimeters from the center of the coupling to the rear end of the vehicle."),
            BaseColumn(input_col="Afstand voorzijde voertuig tot hart koppeling", output_col="front_to_coupling_center_distance", original_type=int, output_data_type=IntegerType(), description="Distance in millimeters from the front of the vehicle to the center of the coupling."),
            BaseColumn(input_col="Afwijkende maximum snelheid", output_col="deviating_max_speed", original_type=int, output_data_type=IntegerType(), description="A different maximum speed than standard for this type of vehicle, expressed in km/h."),
            BaseColumn(input_col="Lengte", output_col="length", original_type=int, output_data_type=IntegerType(), description="Length of the vehicle in millimeters, excluding mirrors."),
            BaseColumn(input_col="Breedte", output_col="width", original_type=int, output_data_type=IntegerType(), description="Width of the vehicle in millimeters, excluding mirrors."),
            BaseColumn(input_col="Europese voertuigcategorie", output_col="european_vehicle_category", original_type=str, output_data_type=StringType(), description="European classification code for the vehicle category, such as M1, N2, O3."),
            BaseColumn(input_col="Europese voertuigcategorie toevoeging", output_col="european_vehicle_category_addition", original_type=str, output_data_type=StringType(), description="Additional European classification data associated with the main vehicle category."),
            BaseColumn(input_col="Europese uitvoeringcategorie toevoeging", output_col="european_variant_category_addition", original_type=str, output_data_type=StringType(), description="Additional information related to the European variant or version of the vehicle."),
            BaseColumn(input_col="Plaats chassisnummer", output_col="chassis_number_location", original_type=str, output_data_type=StringType(), description="Location on the vehicle where the chassis number is physically applied."),
            BaseColumn(input_col="Technische max. massa voertuig", output_col="technical_max_mass", original_type=int, output_data_type=IntegerType(), description="Maximum technically permissible mass of the vehicle in kilograms."),
            BaseColumn(input_col="Type", output_col="type", original_type=str, output_data_type=StringType(), description="Type designation assigned by the manufacturer as part of EU type approval."),
            BaseColumn(input_col="Type gasinstallatie", output_col="gas_installation_type", original_type=str, output_data_type=StringType(), description="Indicates the type of gas installation present in the vehicle, such as LPG or CNG."),
            BaseColumn(input_col="Typegoedkeuringsnummer", output_col="type_approval_number", original_type=str, output_data_type=StringType(), description="Number of the European type approval issued for this vehicle."),
            BaseColumn(input_col="Variant", output_col="variant", original_type=str, output_data_type=StringType(), description="The specific variant designation of the vehicle as defined under EU approval."),
            BaseColumn(input_col="Uitvoering", output_col="version", original_type=str, output_data_type=StringType(), description="Execution specification as recorded in the EU type approval of the vehicle."),
            BaseColumn(input_col="Volgnummer wijziging EU typegoedkeuring", output_col="eu_type_approval_amendment_number", original_type=int, output_data_type=IntegerType(), description="Sequence number of the amendment to the EU type approval applicable to this vehicle."),
            BaseColumn(input_col="Vermogen massarijklaar", output_col="power_to_weight_ratio", original_type=int, output_data_type=IntegerType(), description="Maximum engine power output (kW) related to the vehicle's ready-to-drive weight."),
            BaseColumn(input_col="Wielbasis", output_col="wheelbase", original_type=int, output_data_type=IntegerType(), description="Distance between the centers of the front and rear axles of the vehicle in millimeters."),
            BaseColumn(input_col="Export indicator", output_col="export_indicator", original_type=str, output_data_type=StringType(), description="Yes/No field indicating whether the vehicle has been exported and deregistered in the Netherlands."),
            BaseColumn(input_col="Openstaande terugroepactie indicator", output_col="open_recall_indicator", original_type=str, output_data_type=StringType(), description="Indicates whether the vehicle has an unresolved manufacturer recall as reported to RDW."),
            BaseColumn(input_col="Vervaldatum tachograaf", output_col="tachograph_expiry_date", original_type=str, output_data_type=DateType(), description="Expiration date of the calibration validity of the tachograph installed in the vehicle."),
            BaseColumn(input_col="Taxi indicator", output_col="taxi_indicator", original_type=str, output_data_type=StringType(), description="Indicates whether the vehicle is registered as a taxi according to the RDW records."),
            BaseColumn(input_col="Maximum massa samenstelling", output_col="max_combination_mass", original_type=int, output_data_type=IntegerType(), description="The maximum allowed mass of the vehicle and trailer combination as determined by the manufacturer."),
            BaseColumn(input_col="Aantal rolstoelplaatsen", output_col="number_of_wheelchair_spaces", original_type=int, output_data_type=IntegerType(), description="The number of wheelchair spaces for passengers that are technically and legally permitted."),
            BaseColumn(input_col="Maximum ondersteunende snelheid", output_col="max_assisted_speed", original_type=int, output_data_type=IntegerType(), description="The maximum speed at which electric pedal support is provided, in km/h."),
            BaseColumn(input_col="Jaar laatste registratie tellerstand", output_col="last_odometer_registration_year", original_type=int, output_data_type=IntegerType(), description="The year in which the last mileage reading was registered."),
            BaseColumn(input_col="Tellerstandoordeel", output_col="odometer_judgment", original_type=str, output_data_type=StringType(), description="Judgment of the registered mileage data: logical, possibly illogical, or judgement not possible."),
            BaseColumn(input_col="Code toelichting tellerstandoordeel", output_col="odometer_judgment_explanation_code", original_type=int, output_data_type=StringType(), is_foreign_key=True, foreign_key_reference_table="odometer_reading_judgment_explanation", description="Code linked to the explanation of the mileage judgment."),
            BaseColumn(input_col="Tenaamstellen mogelijk", output_col="registration_possible", original_type=str, output_data_type=StringType(), description="Indicates whether the vehicle is currently eligible for transfer of ownership."),
            BaseColumn(input_col="Maximum last onder de vooras(sen) (tezamen)/koppeling", output_col="max_load_front_axles", original_type=int, output_data_type=IntegerType(), description="The maximum permissible load on the front axle(s) or the front coupling, in kg."),
            BaseColumn(input_col="Type remsysteem voertuig code", output_col="brake_system_type_code", original_type=str, output_data_type=StringType(), description="Code representing the vehicle's brake system configuration."),
            BaseColumn(input_col="Rupsonderstelconfiguratiecode", output_col="crawler_chassis_config_code", original_type=str, output_data_type=StringType(), description="Code indicating the type or configuration of the crawler chassis system."),
            BaseColumn(input_col="Wielbasis voertuig minimum", output_col="wheelbase_minimum", original_type=int, output_data_type=IntegerType(), description="Minimum adjustable wheelbase of the vehicle, in millimeters."),
            BaseColumn(input_col="Wielbasis voertuig maximum", output_col="wheelbase_maximum", original_type=int, output_data_type=IntegerType(), description="Maximum adjustable wheelbase of the vehicle, in millimeters."),
            BaseColumn(input_col="Lengte voertuig minimum", output_col="length_minimum", original_type=int, output_data_type=IntegerType(), description="Minimum adjustable length of the vehicle, in millimeters."),
            BaseColumn(input_col="Lengte voertuig maximum", output_col="length_maximum", original_type=int, output_data_type=IntegerType(), description="Maximum adjustable length of the vehicle, in millimeters."),
            BaseColumn(input_col="Breedte voertuig minimum", output_col="width_minimum", original_type=int, output_data_type=IntegerType(), description="Minimum adjustable width of the vehicle, in millimeters."),
            BaseColumn(input_col="Breedte voertuig maximum", output_col="width_maximum", original_type=int, output_data_type=IntegerType(), description="Maximum adjustable width of the vehicle, in millimeters."),
            BaseColumn(input_col="Hoogte voertuig", output_col="height", original_type=int, output_data_type=IntegerType(), description="Height of the vehicle in millimeters as recorded in the type approval."),
            BaseColumn(input_col="Hoogte voertuig minimum", output_col="height_minimum", original_type=int, output_data_type=IntegerType(), description="Minimum adjustable height of the vehicle, in millimeters."),
            BaseColumn(input_col="Hoogte voertuig maximum", output_col="height_maximum", original_type=int, output_data_type=IntegerType(), description="Maximum adjustable height of the vehicle, in millimeters."),
            BaseColumn(input_col="Massa bedrijfsklaar minimaal", output_col="operating_mass_minimum", original_type=int, output_data_type=IntegerType(), description="Minimum curb weight (operating mass) including driver, fluids, and equipment."),
            BaseColumn(input_col="Massa bedrijfsklaar maximaal", output_col="operating_mass_maximum", original_type=int, output_data_type=IntegerType(), description="Maximum curb weight (operating mass) including driver, fluids, and equipment."),
            BaseColumn(input_col="Technisch toelaatbaar massa koppelpunt", output_col="technical_coupling_point_mass", original_type=int, output_data_type=IntegerType(), description="The maximum technically permissible vertical load on the coupling point."),
            BaseColumn(input_col="Maximum massa technisch maximaal", output_col="technical_max_mass_maximum", original_type=int, output_data_type=IntegerType(), description="Maximum technically allowed total vehicle mass according to the manufacturer."),
            BaseColumn(input_col="Maximum massa technisch minimaal", output_col="technical_max_mass_minimum", original_type=int, output_data_type=IntegerType(), description="Minimum technically allowed total vehicle mass according to the manufacturer."),
            BaseColumn(input_col="Subcategorie Nederland", output_col="subcategory_netherlands", original_type=str, output_data_type=StringType(), description="Dutch vehicle subcategory according to national classification."),
            BaseColumn(input_col="Verticale belasting koppelpunt getrokken voertuig", output_col="vertical_load_coupling_towed_vehicle", original_type=int, output_data_type=IntegerType(), description="Permissible vertical load at the coupling point of the towed vehicle."),
            BaseColumn(input_col="Zuinigheidsclassificatie", output_col="fuel_efficiency_class", original_type=str, output_data_type=StringType(), description="Indicates the fuel efficiency class of the vehicle based on environmental performance."),
            BaseColumn(input_col="API Gekentekende_voertuigen_assen", output_col="api_registered_vehicles_axles", original_type=str, output_data_type=StringType(), description="API endpoint link for axle-related vehicle registration data."),
            BaseColumn(input_col="API Gekentekende_voertuigen_brandstof", output_col="api_registered_vehicles_fuel", original_type=str, output_data_type=StringType(), description="API endpoint link for fuel-related vehicle registration data."),
            BaseColumn(input_col="API Gekentekende_voertuigen_carrosserie", output_col="api_registered_vehicles_body", original_type=str, output_data_type=StringType(), description="API endpoint link for vehicle body type data."),
            BaseColumn(input_col="API Gekentekende_voertuigen_carrosserie_specifiek", output_col="api_registered_vehicles_body_specific", original_type=str, output_data_type=StringType(), description="API endpoint link for specific body type data."),
            BaseColumn(input_col="API Gekentekende_voertuigen_voertuigklasse", output_col="api_registered_vehicles_vehicle_class", original_type=str, output_data_type=StringType(), description="API endpoint link for vehicle class data."),
            BaseColumn(input_col="Vervaldatum APK DT", output_col="mot_expiry_date_dt", original_type=str, output_data_type=StringType(), description="Expiration date of APK (technical inspection) in DT format."),
            BaseColumn(input_col="Datum tenaamstelling DT", output_col="registration_date_dt", original_type=str, output_data_type=StringType(), description="Date of vehicle registration in the Netherlands (DT format)."),
            BaseColumn(input_col="Datum eerste toelating DT", output_col="first_admission_date_dt", original_type=str, output_data_type=StringType(), description="Date of first vehicle approval (DT format)."),
            BaseColumn(input_col="Datum eerste tenaamstelling in Nederland DT", output_col="first_registration_date_nl_dt", original_type=str, output_data_type=StringType(), description="Date of first registration in the Netherlands (DT format)."),
            BaseColumn(input_col="Vervaldatum tachograaf DT", output_col="tachograph_expiry_date_dt", original_type=str, output_data_type=StringType(), description="Expiration date of tachograph calibration (DT format)."),
            BaseColumn(input_col="Registratie datum goedkeuring (afschrijvingsmoment BPM)", output_col="approval_registration_date_bpm_depreciation", original_type=str, output_data_type=StringType(), description="Registration date of approval (BPM depreciation moment)."),
            BaseColumn(input_col="Registratie datum goedkeuring (afschrijvingsmoment BPM) DT", output_col="approval_registration_date_bpm_depreciation_dt", original_type=str, output_data_type=StringType(), description="Registration date of approval (BPM depreciation moment) in DT format."),
            BaseColumn(input_col="Gemiddelde Lading Waarde", output_col="average_load_value", original_type=str, output_data_type=StringType(), description="Average cargo/load value."),
            BaseColumn(input_col="Aerodynamische voorziening of uitrusting", output_col="aerodynamic_equipment", original_type=str, output_data_type=StringType(), description="Aerodynamic equipment or installation present on the vehicle."),
            BaseColumn(input_col="Additionele massa alternatieve aandrijving", output_col="additional_mass_alternative_drive", original_type=str, output_data_type=StringType(), description="Additional mass due to alternative drive system."),
            BaseColumn(input_col="Verlengde cabine indicator", output_col="extended_cabin_indicator", original_type=str, output_data_type=StringType(), description="Indicator if the vehicle has an extended cabin."),
            BaseColumn(input_col="Aantal passagiers zitplaatsen wettelijk", output_col="legal_passenger_seats", original_type=str, output_data_type=StringType(), description="Number of legally approved passenger seats."),
            BaseColumn(input_col="Aanwijzingsnummer", output_col="designation_number", original_type=str, output_data_type=StringType(), description="Reference or identification number for the vehicle.")
        ]
    ),

    RDWTable(
        url="https://opendata.rdw.nl/api/views/3huj-srit/rows.csv?accessType=DOWNLOAD",
        name="registered_vehicles_axles",
        description="Axle information for registered vehicles. Contains details about number of axles, driven axles, braking systems, axle loads, and track widths.",
        columns=[
            BaseColumn(input_col="Kenteken", output_col="license_plate", original_type=str, output_data_type=StringType(), is_foreign_key=True, foreign_key_reference_table="registered_vehicles", description="The license plate of a vehicle, consisting of a combination of numbers and letters. This makes the vehicle unique and identifiable."),
            BaseColumn(input_col="As nummer", output_col="axle_number", original_type=int, output_data_type=IntegerType(), description="The number of the (physical) axle counted from the front of the vehicle. For pendulum axles, the number is determined from left to right, as seen from the driving direction."),
            BaseColumn(input_col="Aantal assen", output_col="number_of_axles", original_type=int, output_data_type=IntegerType(), description="The number of axles of a vehicle."),
            BaseColumn(input_col="Aangedreven as", output_col="driven_axle", original_type=str, output_data_type=StringType(), description="Yes/No indicator that indicates whether the respective axle is driven or not. This data is only relevant for commercial vehicles and buses with a legally permitted maximum mass over 3500 kg."),
            BaseColumn(input_col="Hefas", output_col="lift_axle", original_type=str, output_data_type=StringType(), description="Indicator showing whether an axle can be lifted in such a way that the wheels do not touch the road surface while driving."),
            BaseColumn(input_col="Plaatscode as", output_col="axle_position_code", original_type=str, output_data_type=StringType(), description="Code indicating whether the vehicle has a front axle or rear axle."),
            BaseColumn(input_col="Spoorbreedte", output_col="track_width", original_type=int, output_data_type=IntegerType(), description="The track width of an axle of a vehicle: the horizontal distance between the center of the left and right wheel of that axle, measured on the road surface."),
            BaseColumn(input_col="Weggedrag code", output_col="road_behavior_code", original_type=str, output_data_type=StringType(), description="Code indicating the driving behavior of a driven axle. The following codes are used: L = Air suspension, G = Equivalent to air suspension, A = Other than air suspension. This data is only recorded for commercial vehicles and buses with a legally permitted maximum mass above 3500 kg."),
            BaseColumn(input_col="Wettelijk toegestane maximum aslast", output_col="legal_max_axle_load", original_type=int, output_data_type=IntegerType(), description="The legally permitted maximum mass on the axle, derived from the technically permissible maximum mass. If necessary, this has been reduced by legal provisions or at the request of the applicant of the registration certificate."),
            BaseColumn(input_col="Technisch toegestane maximum aslast", output_col="technical_max_axle_load", original_type=int, output_data_type=IntegerType(), description="The technically permissible maximum mass of the axle, as specified by the manufacturer of the vehicle."),
            BaseColumn(input_col="Geremde as indicator", output_col="braked_axle_indicator", original_type=str, output_data_type=StringType(), description="Indicator showing whether a particular axle of a vehicle is braked."),
            BaseColumn(input_col="Afstand tot volgende as voertuig", output_col="distance_to_next_axle", original_type=int, output_data_type=IntegerType(), description="Distance in centimeters from one axle of a vehicle to the next axle (counted front to back). Not applicable for the last (rear) axle of a vehicle. Values in millimeters are rounded to centimeters."),
            BaseColumn(input_col="Afstand tot volgende as voertuig minimum", output_col="distance_to_next_axle_minimum", original_type=int, output_data_type=IntegerType(), description="Distance in centimeters from one axle of a vehicle to the next axle (counted front to back). Not applicable for the last (rear) axle of a vehicle. Values in millimeters are rounded to centimeters."),
            BaseColumn(input_col="Afstand tot volgende as voertuig maximum", output_col="distance_to_next_axle_maximum", original_type=int, output_data_type=IntegerType(), description="Distance in centimeters from one axle of a vehicle to the next axle (counted front to back). Not applicable for the last (rear) axle. This is the upper limit of a variable dimension due to adjustability. Not to be confused with ranges in type approval. Values in millimeters are rounded to centimeters."),
            BaseColumn(input_col="Maximum last as technisch maximum", output_col="max_axle_load_technical_maximum", original_type=int, output_data_type=IntegerType(), description="The maximum technically permissible load on an axle, depending on the setting of the variable axle."),
            BaseColumn(input_col="Maximum last as technisch minimum", output_col="max_axle_load_technical_minimum", original_type=int, output_data_type=IntegerType(), description="The minimum technically permissible load on an axle, depending on the setting of the variable axle.")
        ]
    ),

    RDWTable(
        url="https://opendata.rdw.nl/api/views/8ys7-d773/rows.csv?accessType=DOWNLOAD",
        name = "registered_vehicles_fuel",
        description="Fuel and emissions data for registered vehicles. Contains information about fuel types, consumption rates, CO2 emissions, noise levels, emission classes, and WLTP test results.",
        columns=[
            BaseColumn(input_col="Kenteken", output_col="license_plate", original_type=str, output_data_type=StringType(), is_foreign_key=True, foreign_key_reference_table="registered_vehicles", description="The license plate of a vehicle, consisting of a combination of numbers and letters. This makes the vehicle unique and identifiable."),
            BaseColumn(input_col="Brandstof volgnummer", output_col="fuel_sequence_number", original_type=str, output_data_type=IntegerType(), description="Sequence number used to show emission data for a specific fuel in the desired order."),
            BaseColumn(input_col="Brandstof omschrijving", output_col="fuel_description", original_type=str, output_data_type=StringType(), description="National code for the fuel or energy source of the vehicle's engine."),
            BaseColumn(input_col="Brandstofverbruik buiten de stad", output_col="fuel_consumption_extra_urban", original_type=int, output_data_type=IntegerType(), description="Fuel consumption in l/100 km, during a standardized out-of-town trip, tested on a roller bench."),
            BaseColumn(input_col="Brandstofverbruik gecombineerd", output_col="fuel_consumption_combined", original_type=int, output_data_type=IntegerType(), description="Fuel consumption in l/100 km, during a combination of standardized urban and out-of-town trips, tested on a roller bench."),
            BaseColumn(input_col="Brandstofverbruik stad", output_col="fuel_consumption_urban", original_type=int, output_data_type=IntegerType(), description="Fuel consumption in l/100 km, during a standardized urban cycle trip, tested on a roller bench."),
            BaseColumn(input_col="CO2 uitstoot gecombineerd", output_col="co2_emission_combined", original_type=int, output_data_type=IntegerType(), description="The weighted CO2 emission in g/km of a plug-in hybrid vehicle, during a combination of a city trip and a trip outside the city, tested on a roller bench."),
            BaseColumn(input_col="CO2 uitstoot gewogen", output_col="co2_emission_weighted", original_type=int, output_data_type=IntegerType(), description="CO2 emission measured on a roller bench for an externally chargeable hybrid electric vehicle, averaged from driving once with empty and once with full batteries."),
            BaseColumn(input_col="Geluidsniveau rijdend", output_col="noise_level_driving", original_type=int, output_data_type=IntegerType(), description="The noise level of a moving vehicle in dB(A), measured as prescribed by regulations. Not recorded for electric and plug-in hybrid vehicles."),
            BaseColumn(input_col="Geluidsniveau stationair", output_col="noise_level_stationary", original_type=int, output_data_type=IntegerType(), description="The noise level of a stationary vehicle with running engine in dB(A), measured at the corresponding engine speed as prescribed. Not recorded for electric and plug-in hybrid vehicles."),
            BaseColumn(input_col="Emissieklasse", output_col="emission_class", original_type=str, output_data_type=StringType(), description="The environmental classification recorded by RDW, such as Euro 0 to Euro 6, EEV, and R. Indicates the level of pollutant emissions."),
            BaseColumn(input_col="Milieuklasse EG Goedkeuring (licht)", output_col="environmental_class_ec_approval_light", original_type=str, output_data_type=StringType(), description="Directive number for emissions from vehicles (light-duty) on which the vehicle was tested and approved."),
            BaseColumn(input_col="Milieuklasse EG Goedkeuring (zwaar)", output_col="environmental_class_ec_approval_heavy", original_type=str, output_data_type=StringType(), description="Directive number for emissions from engines (heavy-duty) on which the vehicle was tested and approved."),
            BaseColumn(input_col="Uitstoot deeltjes (licht)", output_col="particulate_emission_light", original_type=int, output_data_type=IntegerType(), description="Emission of the number of particles, expressed in g/km, measured during a test on a roller bench (light-duty)."),
            BaseColumn(input_col="Uitstoot deeltjes (zwaar)", output_col="particulate_emission_heavy", original_type=str, output_data_type=StringType(), description="Particulate emissions for heavy vehicles."),
            BaseColumn(input_col="Nettomaximumvermogen", output_col="net_maximum_power", original_type=str, output_data_type=StringType(), description="Net maximum power of the vehicle."),
            BaseColumn(input_col="Nominaal continu maximumvermogen", output_col="nominal_continuous_max_power", original_type=str, output_data_type=StringType(), description="Nominal continuous maximum power of the vehicle."),
            BaseColumn(input_col="Roetuitstoot", output_col="soot_emission", original_type=str, output_data_type=StringType(), description="Soot emissions of the vehicle."),
            BaseColumn(input_col="Toerental geluidsniveau", output_col="engine_speed_noise_level", original_type=str, output_data_type=StringType(), description="Engine speed at noise measurement."),
            BaseColumn(input_col="Emissie deeltjes type1 wltp", output_col="particulate_emission_type1_wltp", original_type=str, output_data_type=StringType(), description="Type 1 particulate emissions measured with WLTP."),
            BaseColumn(input_col="Emissie co2 gecombineerd wltp", output_col="co2_emission_combined_wltp", original_type=str, output_data_type=StringType(), description="Combined CO2 emissions measured with WLTP."),
            BaseColumn(input_col="Emissie co2 gewogen gecombineerd wltp", output_col="co2_emission_weighted_combined_wltp", original_type=str, output_data_type=StringType(), description="Weighted combined CO2 emissions measured with WLTP."),
            BaseColumn(input_col="Brandstof verbruik gecombineerd wltp", output_col="fuel_consumption_combined_wltp", original_type=str, output_data_type=StringType(), description="Combined fuel consumption measured with WLTP."),
            BaseColumn(input_col="Brandstof verbruik gewogen gecombineerd wltp", output_col="fuel_consumption_weighted_combined_wltp", original_type=str, output_data_type=StringType(), description="Weighted combined fuel consumption measured with WLTP."),
            BaseColumn(input_col="Elektrisch verbruik enkel elektrisch wltp", output_col="electric_consumption_pure_electric_wltp", original_type=str, output_data_type=StringType(), description="Electric consumption in pure electric mode measured with WLTP."),
            BaseColumn(input_col="Actie radius enkel elektrisch wltp", output_col="range_pure_electric_wltp", original_type=str, output_data_type=StringType(), description="Action radius in pure electric mode measured with WLTP."),
            BaseColumn(input_col="Actie radius enkel elektrisch stad wltp", output_col="range_pure_electric_urban_wltp", original_type=str, output_data_type=StringType(), description="Action radius in pure electric mode in city driving (WLTP)."),
            BaseColumn(input_col="Elektrisch verbruik extern opladen wltp", output_col="electric_consumption_external_charging_wltp", original_type=str, output_data_type=StringType(), description="Electric consumption when externally charging measured with WLTP."),
            BaseColumn(input_col="Actie radius extern opladen wltp", output_col="range_external_charging_wltp", original_type=str, output_data_type=StringType(), description="Action radius when externally charging measured with WLTP."),
            BaseColumn(input_col="Actie radius extern opladen stad wltp", output_col="range_external_charging_urban_wltp", original_type=str, output_data_type=StringType(), description="Action radius in city driving when externally charging (WLTP)."),
            BaseColumn(input_col="Max vermogen 15 minuten", output_col="max_power_15_minutes", original_type=str, output_data_type=StringType(), description="Maximum power over 15 minutes."),
            BaseColumn(input_col="Max vermogen 60 minuten", output_col="max_power_60_minutes", original_type=str, output_data_type=StringType(), description="Maximum power over 60 minutes."),
            BaseColumn(input_col="Netto max vermogen elektrisch", output_col="net_max_power_electric", original_type=str, output_data_type=StringType(), description="Net maximum electric power."),
            BaseColumn(input_col="Klasse hybride elektrisch voertuig", output_col="hybrid_electric_vehicle_class", original_type=str, output_data_type=StringType(), description="Class of hybrid electric vehicle."),
            BaseColumn(input_col="Opgegeven maximum snelheid", output_col="declared_max_speed", original_type=str, output_data_type=StringType(), description="Declared maximum speed of the vehicle."),
            BaseColumn(input_col="Uitlaatemissieniveau", output_col="exhaust_emission_level", original_type=str, output_data_type=StringType(), description="Exhaust emission level."),
            BaseColumn(input_col="CO2 emissieklasse", output_col="co2_emission_class", original_type=str, output_data_type=StringType(), description="CO2 emission class of the vehicle.")

        ]
    ),


    RDWTable(
        url="https://opendata.rdw.nl/api/views/vezc-m2t6/rows.csv?accessType=DOWNLOAD",
        name = "registered_vehicles_body",
        description="Body type information for registered vehicles. Contains European body type classifications and descriptions as determined during vehicle type approval.",
        columns=[
            BaseColumn(input_col="Kenteken", output_col="license_plate", original_type=str, output_data_type=StringType(), is_foreign_key=True, foreign_key_reference_table="registered_vehicles", description="The license plate of a vehicle, consisting of a combination of numbers and letters. This makes the vehicle unique and identifiable."),
            BaseColumn(input_col="Carrosserie volgnummer", output_col="body_sequence_number", original_type=str, output_data_type=IntegerType(), description="Sequence number for the body type."),
            BaseColumn(input_col="Carrosserietype", output_col="body_type", original_type=str, output_data_type=StringType(), description="European designation for the body type of a vehicle. This is determined during the approval of a vehicle."),
            BaseColumn(input_col="Type Carrosserie Europese omschrijving", output_col="body_type_european_description", original_type=str, output_data_type=StringType(), description="European description of the type of bodywork.")
        ]
    ),


    RDWTable(
        url="https://opendata.rdw.nl/api/views/jhie-znh9/rows.csv?accessType=DOWNLOAD",
        name = "registered_vehicles_body_specification",
        description="Detailed body type specifications for registered vehicles. Contains European body codes and detailed descriptions for approved complete or completed vehicles of categories M, N, or O.",
        columns=[
            BaseColumn(input_col="Kenteken", output_col="license_plate", original_type=str, output_data_type=StringType(), is_foreign_key=True, foreign_key_reference_table="registered_vehicles", description="The license plate of a vehicle, consisting of a combination of numbers and letters. This makes the vehicle unique and identifiable."),
            BaseColumn(input_col="Carrosserie volgnummer", output_col="body_sequence_number", original_type=str, output_data_type=IntegerType(), description="Sequence number for the body type."),
            BaseColumn(input_col="Carrosserie voertuig nummer code volgnummer", output_col="body_vehicle_number_code_sequence", original_type=int, output_data_type=IntegerType(), description="Sequence number of the code associated with the vehicle body number."),
            BaseColumn(input_col="Carrosseriecode", output_col="body_code", original_type=int, output_data_type=IntegerType(), description="European code for the type of bodywork of an approved complete or completed vehicle of category M, N, or O."),
            BaseColumn(input_col="Carrosserie voertuig nummer Europese omschrijving", output_col="body_vehicle_number_european_description", original_type=str, output_data_type=StringType(), description="European description of the code associated with the vehicle body number.")
        ]
    ),


    RDWTable(
        url="https://opendata.rdw.nl/api/views/kmfi-hrps/rows.csv?accessType=DOWNLOAD",
        name = "registered_vehicles_class",
        description="Vehicle class information for buses. Contains European vehicle class codes and descriptions established during type approval, indicating which classes the vehicle is approved for (M2 and M3 categories).",
        columns=[
            BaseColumn(input_col="Kenteken", output_col="license_plate", original_type=str, output_data_type=StringType(), is_foreign_key=True, foreign_key_reference_table="registered_vehicles", description="The license plate of a vehicle, consisting of a combination of numbers and letters. This makes the vehicle unique and identifiable."),
            BaseColumn(input_col="Carrosserie volgnummer", output_col="body_sequence_number", original_type=str, output_data_type=IntegerType(), description="Sequence number for the body type."),
            BaseColumn(input_col="Carrosserie klasse volgnummer", output_col="body_class_sequence_number", original_type=int, output_data_type=IntegerType(), description="Sequence number indicating a change to a class configuration."),
            BaseColumn(input_col="Voertuigklasse", output_col="vehicle_class", original_type=str, output_data_type=StringType(), description="Vehicle class established during the approval process, indicating the classes the vehicle is approved for."),
            BaseColumn(input_col="Voertuigklasse omschrijving", output_col="vehicle_class_description", original_type=str, output_data_type=StringType(), description="Description of the vehicle class according to the European classification for buses.")
        ]
    ),


    RDWTable(
        url="https://opendata.rdw.nl/api/views/2ba7-embk/rows.csv?accessType=DOWNLOAD",
        name = "registered_vehicles_subcategory",
        description="Vehicle subcategory information. Contains European coding for special-purpose body subcategories and their descriptions for registered vehicles.",
        columns=[
            BaseColumn(input_col="Kenteken", output_col="license_plate", original_type=str, output_data_type=StringType(), is_foreign_key=True, foreign_key_reference_table="registered_vehicles", description="The license plate of a vehicle, consisting of a combination of numbers and letters. This makes the vehicle unique and identifiable."),
            BaseColumn(input_col="Subcategorie voertuig volgnummer", output_col="vehicle_subcategory_sequence_number", original_type=int, output_data_type=IntegerType(), description="Sequence number indicating a change to a vehicle's subcategory."),
            BaseColumn(input_col="Subcategorie voertuig europees", output_col="vehicle_subcategory_european", original_type=str, output_data_type=StringType(), description="European coding for special-purpose body subcategory of a vehicle."),
            BaseColumn(input_col="Subcategorie voertuig europees omschrijving", output_col="vehicle_subcategory_european_description", original_type=str, output_data_type=StringType(), description="Description of the European vehicle subcategory.")
        ]
    ),


    RDWTable(
        url="https://opendata.rdw.nl/api/views/7ug8-2dtt/rows.csv?accessType=DOWNLOAD",
        name = "registered_vehicles_special_features",
        description="Special features and peculiarities registered for vehicles. Contains codes, descriptions, variable text values, and units for various vehicle-specific characteristics and exceptions.",
        columns=[
            BaseColumn(input_col="Kenteken", output_col="license_plate", original_type=str, output_data_type=StringType(), is_foreign_key=True, foreign_key_reference_table="registered_vehicles", description="The license plate of a vehicle, consisting of a combination of numbers and letters. This makes the vehicle unique and identifiable."),
            BaseColumn(input_col="Bijzonderheid volgnummer", output_col="special_feature_sequence_number", original_type=int, output_data_type=IntegerType(), description="The logical sequence number of a recorded peculiarity."),
            BaseColumn(input_col="Bijzonderheid code", output_col="special_feature_code", original_type=int, output_data_type=IntegerType(), description="Code of a peculiarity registered for a vehicle."),
            BaseColumn(input_col="Bijzonderheid code omschrijving", output_col="special_feature_code_description", original_type=str, output_data_type=StringType(), description="The description of the peculiarity registered for a vehicle."),
            BaseColumn(input_col="Bijzonderheid variabele tekst", output_col="special_feature_variable_text", original_type=str, output_data_type=StringType(), description="The variable part of a peculiarity that belongs to a peculiarity."),
            BaseColumn(input_col="Bijzonderheid eenheid", output_col="special_feature_unit", original_type=str, output_data_type=StringType(), description="The unit of the variable part of the peculiarity.")
        ]
    ),

    RDWTable(
        url="https://opendata.rdw.nl/api/views/3xwf-ince/rows.csv?accessType=DOWNLOAD",
        name = "registered_vehicles_crawler_tracks",
        description="Crawler track (rubber track) information for registered vehicles. Contains details about track sets, braking indicators, drive indicators, and technically permissible maximum mass per track set.",
        columns=[
            BaseColumn(input_col="Kenteken", output_col="license_plate", original_type=str, output_data_type=StringType(), is_foreign_key=True, foreign_key_reference_table="registered_vehicles", description="The license plate of a vehicle, consisting of a combination of numbers and letters. This makes the vehicle unique and identifiable."),
            BaseColumn(input_col="Rupsband set volgnummer", output_col="crawler_track_set_sequence_number", original_type=int, output_data_type=IntegerType(), description="Sequence number of the rubber track set."),
            BaseColumn(input_col="Geremde rupsband indicator", output_col="braked_crawler_track_indicator", original_type=str, output_data_type=StringType(), description="Indicator specifying which rubber track set is braked."),
            BaseColumn(input_col="Aangedreven rupsband indicator", output_col="driven_crawler_track_indicator", original_type=str, output_data_type=StringType(), description="Indicator specifying which rubber track set is driven."),
            BaseColumn(input_col="Technisch toelaatbaar maximum massa rupsbandset", output_col="technical_max_mass_crawler_track_set", original_type=int, output_data_type=IntegerType(), description="Technically permissible maximum mass per rubber track set."),
            BaseColumn(input_col="Technisch toelaatbaar maximum massa rupsband set minimum", output_col="technical_max_mass_crawler_track_set_minimum", original_type=int, output_data_type=IntegerType(), description="Technically permissible minimum mass per rubber track set."),
            BaseColumn(input_col="Technisch toelaatbaar maximum massa rupsband set maximum", output_col="technical_max_mass_crawler_track_set_maximum", original_type=int, output_data_type=IntegerType(), description="Technically permissible maximum mass per rubber track set.")
        ]
    )

]


def get_table_by_name(table_name: str) -> Optional[RDWTable]:
    """Get a table definition by name.

    Args:
        table_name: Name of the table to find

    Returns:
        RDWTable if found, None otherwise
    """
    for table in rdw_tables:
        if table.name == table_name:
            return table
    return None