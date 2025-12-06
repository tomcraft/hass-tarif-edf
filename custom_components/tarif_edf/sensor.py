from homeassistant.core import HomeAssistant
from homeassistant.config_entries import ConfigEntry
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.device_registry import DeviceEntryType, DeviceInfo
from homeassistant.components.sensor import (
    SensorEntity,
    SensorDeviceClass,
    SensorStateClass,
)

from homeassistant.helpers.update_coordinator import (
    CoordinatorEntity,
)


from .coordinator import TarifEdfDataUpdateCoordinator

from .const import (
    DOMAIN,
    CONTRACT_TYPE_BASE,
    CONTRACT_TYPE_HPHC,
    CONTRACT_TYPE_TEMPO,
    TEMPO_COLORS_MAPPING,
)

async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback
) -> None:
    coordinator: TarifEdfDataUpdateCoordinator = hass.data[DOMAIN][
        config_entry.entry_id
    ]["coordinator"]

    sensors = [
        TarifEdfSensor(coordinator, 'contract_power', f"Puissance souscrite {coordinator.data['contract_type']} {coordinator.data['contract_power']}kVA", 'kVA'),
    ]

    if coordinator.data['contract_type'] == CONTRACT_TYPE_BASE:
        sensors.extend([
            TarifEdfSensor(coordinator, 'base_variable_ttc', 'Tarif Base TTC', 'EUR/kWh', SensorDeviceClass.MONETARY),
            TarifEdfSensor(coordinator, 'base_fixe_ttc', 'Tarif Abonnement Base TTC', 'EUR/mois', SensorDeviceClass.MONETARY),
        ])
    elif coordinator.data['contract_type'] == CONTRACT_TYPE_HPHC:
        sensors.extend([
            TarifEdfSensor(coordinator, 'hphc_variable_hc_ttc', 'Tarif Heures creuses TTC', 'EUR/kWh', SensorDeviceClass.MONETARY),
            TarifEdfSensor(coordinator, 'hphc_variable_hp_ttc', 'Tarif Heures pleines TTC', 'EUR/kWh', SensorDeviceClass.MONETARY),
            TarifEdfSensor(coordinator, 'hphc_fixe_ttc', 'Tarif Abonnement HPHC TTC', 'EUR/mois', SensorDeviceClass.MONETARY),
        ])
    elif coordinator.data['contract_type'] == CONTRACT_TYPE_TEMPO:
        color_options = list(TEMPO_COLORS_MAPPING.values())
        sensors.extend([
            TarifEdfSensor(coordinator, 'tempo_couleur', 'Tarif Tempo Couleur', device_class = SensorDeviceClass.ENUM, enum_options=color_options),
            TarifEdfSensor(coordinator, 'tempo_prochaine_couleur', 'Tarif Tempo Prochaine Couleur', device_class = SensorDeviceClass.ENUM, enum_options=color_options),
            TarifEdfSensor(coordinator, 'tempo_couleur_hier', 'Tarif Tempo Couleur Hier', device_class = SensorDeviceClass.ENUM, enum_options=color_options),
            TarifEdfSensor(coordinator, 'tempo_couleur_aujourdhui', "Tarif Tempo Couleur Aujourd'hui", device_class = SensorDeviceClass.ENUM, enum_options=color_options),
            TarifEdfSensor(coordinator, 'tempo_couleur_demain', 'Tarif Tempo Couleur Demain', device_class = SensorDeviceClass.ENUM, enum_options=color_options),
            TarifEdfSensor(coordinator, 'tempo_variable_hc_ttc', 'Tarif Tempo Heures creuses TTC', 'EUR/kWh', SensorDeviceClass.MONETARY),
            TarifEdfSensor(coordinator, 'tempo_variable_hp_ttc', 'Tarif Tempo Heures pleines TTC', 'EUR/kWh', SensorDeviceClass.MONETARY),
            TarifEdfSensor(coordinator, 'tempo_variable_hc_bleu_ttc', 'Tarif Bleu Tempo Heures creuses TTC', 'EUR/kWh', SensorDeviceClass.MONETARY),
            TarifEdfSensor(coordinator, 'tempo_variable_hp_bleu_ttc', 'Tarif Bleu Tempo Heures pleines TTC', 'EUR/kWh', SensorDeviceClass.MONETARY),
            TarifEdfSensor(coordinator, 'tempo_variable_hc_rouge_ttc', 'Tarif Rouge Tempo Heures creuses TTC', 'EUR/kWh', SensorDeviceClass.MONETARY),
            TarifEdfSensor(coordinator, 'tempo_variable_hp_rouge_ttc', 'Tarif Rouge Tempo Heures pleines TTC', 'EUR/kWh', SensorDeviceClass.MONETARY),
            TarifEdfSensor(coordinator, 'tempo_variable_hc_blanc_ttc', 'Tarif Blanc Tempo Heures creuses TTC', 'EUR/kWh', SensorDeviceClass.MONETARY),
            TarifEdfSensor(coordinator, 'tempo_variable_hp_blanc_ttc', 'Tarif Blanc Tempo Heures pleines TTC', 'EUR/kWh', SensorDeviceClass.MONETARY),
            TarifEdfSensor(coordinator, 'tempo_fixe_ttc', 'Tarif Abonnement Tempo TTC', 'EUR/mois', SensorDeviceClass.MONETARY),

            TarifEdfSensor(coordinator, 'tempo_stats_jours_bleus_restants', 'Jours Tempo Bleus Restants', 'jours'),
            TarifEdfSensor(coordinator, 'tempo_stats_jours_bleus_consommes', 'Jours Tempo Bleus Consommés', 'jours'),
            TarifEdfSensor(coordinator, 'tempo_stats_jours_bleus_total', 'Jours Tempo Bleus Total', 'jours'),

            TarifEdfSensor(coordinator, 'tempo_stats_jours_blancs_restants', 'Jours Tempo Blancs Restants', 'jours'),
            TarifEdfSensor(coordinator, 'tempo_stats_jours_blancs_consommes', 'Jours Tempo Blancs Consommés', 'jours'),
            TarifEdfSensor(coordinator, 'tempo_stats_jours_blancs_total', 'Jours Tempo Blancs Total', 'jours'),

            TarifEdfSensor(coordinator, 'tempo_stats_jours_rouges_restants', 'Jours Tempo Rouges Restants', 'jours'),
            TarifEdfSensor(coordinator, 'tempo_stats_jours_rouges_consommes', 'Jours Tempo Rouges Consommés', 'jours'),
            TarifEdfSensor(coordinator, 'tempo_stats_jours_rouges_total', 'Jours Tempo Rouges Total', 'jours'),
        ])

    if coordinator.data['tarif_actuel_ttc'] is not None:
        sensors.append(
            TarifEdfSensor(coordinator, 'tarif_actuel_ttc', f"Tarif actuel {coordinator.data['contract_type']} {coordinator.data['contract_power']}kVA TTC", 'EUR/kWh', SensorDeviceClass.MONETARY)
        )

    async_add_entities(sensors, False)

class TarifEdfSensor(CoordinatorEntity, SensorEntity):
    """Representation of a Tarif EDF sensor."""

    def __init__(
        self,
        coordinator,
        coordinator_key: str,
        name: str,
        unit_of_measurement: str | None = None,
        device_class: SensorDeviceClass | None = None,
        state_class: SensorStateClass | None = None,
        enum_options: list[str] | None = None
    ) -> None:
        """Initialize the Tarif EDF sensor."""
        super().__init__(coordinator)
        contract_name = str.upper(self.coordinator.data['contract_type']) + " " + self.coordinator.data['contract_power'] + "kVA"

        self._coordinator_key = coordinator_key
        self._name = name
        self._attr_unique_id = f"tarif_edf_{self._name}"
        self._attr_name = name
        self._attr_device_info = DeviceInfo(
            name=f"Tarif EDF - {contract_name}",
            entry_type=DeviceEntryType.SERVICE,
            identifiers={
                (DOMAIN, f"Tarif EDF - {contract_name}")
            },
            manufacturer="Tarif EDF",
            model=contract_name,
        )

        if unit_of_measurement is not None:
            self._attr_native_unit_of_measurement = unit_of_measurement
        if device_class is not None:
            self._attr_device_class = device_class
        if state_class is not None:
            self._attr_state_class = state_class

        if device_class is SensorDeviceClass.ENUM and enum_options is not None:
            self._attr_options = enum_options

    @property
    def native_value(self):
        """Return the state of the sensor."""
        if self.coordinator.data[self._coordinator_key] is None:
            return 'unavailable'
        else:
            return self.coordinator.data[self._coordinator_key]

    @property
    def available(self) -> bool:
        """Return if entity is available."""
        return self.coordinator.last_update_success and self.coordinator.data[self._coordinator_key] is not None
