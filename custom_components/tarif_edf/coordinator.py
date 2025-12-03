"""Data update coordinator for the Tarif EDF integration."""
from __future__ import annotations

from datetime import timedelta, datetime, date
import time
import re
from typing import Any
import json
import logging
import csv
import aiohttp
import async_timeout

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import TimestampDataUpdateCoordinator

from .const import (
    DEFAULT_REFRESH_INTERVAL,
    CONTRACT_TYPE_BASE,
    CONTRACT_TYPE_HPHC,
    CONTRACT_TYPE_TEMPO,
    TARIF_BASE_URL,
    TARIF_HPHC_URL,
    TARIF_TEMPO_URL,
    TEMPO_COLOR_API_URL,
    TEMPO_COLORS_MAPPING,
    TEMPO_DAY_START_AT,
    TEMPO_TOMRROW_AVAILABLE_AT,
    TEMPO_OFFPEAK_HOURS
)

_LOGGER = logging.getLogger(__name__)

async def get_remote_file_async(url: str):
    """Return an URL content async."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"
        )
    }
    timeout = 10  # secondes

    async with aiohttp.ClientSession() as session:
        with async_timeout.timeout(timeout):
            async with session.get(url, headers=headers, allow_redirects=True) as resp:
                resp.raise_for_status()
                # Return brute content
                return await resp.read()

def str_to_time(str):
    return datetime.strptime(str, '%H:%M').time()

def str_to_date(str):
    return datetime.strptime(str, "%d/%m/%Y").date()

def time_in_between(now, start, end):
    if start <= end:
        return start <= now < end
    else:
        return start <= now or now < end

def get_tempo_color_from_code(code):
    return TEMPO_COLORS_MAPPING[code]


class TarifEdfDataUpdateCoordinator(TimestampDataUpdateCoordinator):
    """Data update coordinator for the Tarif EDF integration."""

    config_entry: ConfigEntry
    tempo_cache = {}

    def __init__(
        self, hass: HomeAssistant, entry: ConfigEntry
    ) -> None:
        """Initialize the coordinator."""
        super().__init__(
            hass=hass,
            logger=_LOGGER,
            name=entry.title,
            update_interval=timedelta(minutes=1),
        )
        self.config_entry = entry

    def clear_tempo_cache(self, today):
        expired_date_str = (today - timedelta(days=2)).strftime('%Y-%m-%d')
        if expired_date_str in self.tempo_cache:
            del self.tempo_cache[expired_date_str]

    async def get_tempo_day(self, date):
        date_str = date.strftime('%Y-%m-%d')
        now = datetime.now()

        if date_str in self.tempo_cache:
            cached_data = self.tempo_cache[date_str]
            day_start_at = datetime.combine(date, str_to_time(TEMPO_DAY_START_AT))
            is_undefined_color = cached_data['codeJour'] == 0

            # If the data is complete and was fetched after the day started, keep the value
            if not is_undefined_color and cached_data['__cached_at'] > day_start_at:
                return cached_data

            # Otherwise, regularly update it (not too often)
            refresh_period = timedelta(minutes=60)

            # If the data is still not complete, update a bit more often
            if is_undefined_color:
                available_at = datetime.combine(date - timedelta(days=1), str_to_time(TEMPO_TOMRROW_AVAILABLE_AT))
                if now < available_at:
                    # The data is not expected to be available, update only twice an hour
                    refresh_period = timedelta(minutes=30)
                else:
                    # The data is expected to be available, update 4 times per hour
                    refresh_period = timedelta(minutes=15)

            # Use the cached data until expiration
            if cached_data['__cached_at'] + refresh_period > now:
                return cached_data

        # Query data from API
        url = f"{TEMPO_COLOR_API_URL}/{date_str}"
        response_bytes = await get_remote_file_async(url)
        response_json = json.loads(response_bytes.decode('utf-8'))
        response_json['__cached_at'] = now

        # Cache the result
        self.tempo_cache[date_str] = response_json

        return response_json

    async def _async_update_data(self) -> dict[Platform, dict[str, Any]]:
        """Get the latest data from Tarif EDF and updates the state."""
        data = self.config_entry.data
        previous_data = None if self.data is None else self.data.copy()
        now = datetime.now()
        today = now.date()

        if self.data is None:
            self.data = {
                "contract_power": data['contract_power'],
                "contract_type": data['contract_type'],
                "last_refresh_at": None,
                "tarif_actuel_ttc": None
            }

        fresh_data_limit = now - timedelta(days=self.config_entry.options.get("refresh_interval", DEFAULT_REFRESH_INTERVAL))

        tarif_needs_update = self.data['last_refresh_at'] is None or self.data['last_refresh_at'] < fresh_data_limit

        self.logger.debug('EDF tarif_needs_update '+('yes' if tarif_needs_update else 'no'))

        if tarif_needs_update:
            if data['contract_type'] == CONTRACT_TYPE_BASE:
                url = TARIF_BASE_URL
            elif data['contract_type'] == CONTRACT_TYPE_HPHC:
                    url = TARIF_HPHC_URL
            elif data['contract_type'] == CONTRACT_TYPE_TEMPO:
                    url = TARIF_TEMPO_URL

            response = await get_remote_file_async(url)
            lines = response.decode('utf-8').splitlines()
            reader = csv.DictReader(lines, delimiter=';')

            # Reverse list of rows, so we read from the last line
            for row in reversed(list(reader)):
                if row['DATE_DEBUT'] == '':  # CSV can contain empty lines
                    continue
                #Prices are defined into an interval with a begin date and an optional end date
                beginDate = str_to_date(row['DATE_DEBUT'])
                if today < beginDate:
                    continue
                endDate = str_to_date(row['DATE_FIN']) if row['DATE_FIN']  != '' else None
                if endDate is not None and endDate < today:
                    continue
                if row['P_SOUSCRITE'] == data['contract_power']:
                    if data['contract_type'] == CONTRACT_TYPE_BASE:
                        self.data['base_fixe_ttc'] = float(row['PART_FIXE_TTC'].replace(",", "." )) / 12
                        self.data['base_variable_ttc'] = float(row['PART_VARIABLE_TTC'].replace(",", "." ))
                    elif data['contract_type'] == CONTRACT_TYPE_HPHC:
                        self.data['hphc_fixe_ttc'] = float(row['PART_FIXE_TTC'].replace(",", "." )) / 12
                        self.data['hphc_variable_hc_ttc'] = float(row['PART_VARIABLE_HC_TTC'].replace(",", "." ))
                        self.data['hphc_variable_hp_ttc'] = float(row['PART_VARIABLE_HP_TTC'].replace(",", "." ))
                    elif data['contract_type'] == CONTRACT_TYPE_TEMPO:
                        self.data['tempo_fixe_ttc'] = float(row['PART_FIXE_TTC'].replace(",", "." )) / 12
                        self.data['tempo_variable_hc_bleu_ttc'] = float(row['PART_VARIABLE_HCBleu_TTC'].replace(",", "." ))
                        self.data['tempo_variable_hp_bleu_ttc'] = float(row['PART_VARIABLE_HPBleu_TTC'].replace(",", "." ))
                        self.data['tempo_variable_hc_blanc_ttc'] = float(row['PART_VARIABLE_HCBlanc_TTC'].replace(",", "." ))
                        self.data['tempo_variable_hp_blanc_ttc'] = float(row['PART_VARIABLE_HPBlanc_TTC'].replace(",", "." ))
                        self.data['tempo_variable_hc_rouge_ttc'] = float(row['PART_VARIABLE_HCRouge_TTC'].replace(",", "." ))
                        self.data['tempo_variable_hp_rouge_ttc'] = float(row['PART_VARIABLE_HPRouge_TTC'].replace(",", "." ))

                    self.data['last_refresh_at'] = now

                    break

        if data['contract_type'] == CONTRACT_TYPE_TEMPO:
            yesterday = today - timedelta(days=1)
            tomorrow = today + timedelta(days=1)

            if self.data['last_refresh_at'] is not None and self.data['last_refresh_at'].date() != today:
                self.clear_tempo_cache(today)

            import asyncio
            tempo_yesterday, tempo_today, tempo_tomorrow = await asyncio.gather(
                self.get_tempo_day(yesterday),
                self.get_tempo_day(today),
                self.get_tempo_day(tomorrow),
            )

            self.logger.debug('EDF Tempo Cache')
            self.logger.debug(self.tempo_cache)

            if now.time() < str_to_time(TEMPO_DAY_START_AT):
                tempo_now, tempo_next = tempo_yesterday, tempo_today
            else:
                tempo_now, tempo_next = tempo_today, tempo_tomorrow

            yesterday_color = get_tempo_color_from_code(tempo_yesterday['codeJour'])
            today_color = get_tempo_color_from_code(tempo_today['codeJour'])
            tomorrow_color = get_tempo_color_from_code(tempo_tomorrow['codeJour'])
            
            now_color = get_tempo_color_from_code(tempo_now['codeJour'])
            next_color = get_tempo_color_from_code(tempo_next['codeJour'])

            self.data['tempo_couleur_hier'] = yesterday_color
            self.data['tempo_couleur_aujourdhui'] = today_color
            self.data['tempo_couleur_demain'] = tomorrow_color
            self.data['tempo_couleur'] = now_color
            self.data['tempo_prochaine_couleur'] = next_color

            if tempo_now['codeJour'] in [1, 2, 3]:
                self.data['tempo_variable_hp_ttc'] = self.data[f"tempo_variable_hp_{now_color}_ttc"]
                self.data['tempo_variable_hc_ttc'] = self.data[f"tempo_variable_hc_{now_color}_ttc"]
            
            self.data['last_refresh_at'] = now

        default_offpeak_hours = None
        if data['contract_type'] == CONTRACT_TYPE_TEMPO:
            default_offpeak_hours = TEMPO_OFFPEAK_HOURS
        off_peak_hours_ranges = self.config_entry.options.get("off_peak_hours_ranges", default_offpeak_hours)

        if data['contract_type'] == CONTRACT_TYPE_BASE:
            self.data['tarif_actuel_ttc'] = self.data['base_variable_ttc']
        elif data['contract_type'] in [CONTRACT_TYPE_HPHC, CONTRACT_TYPE_TEMPO] and off_peak_hours_ranges is not None:
            contract_type_key = 'hphc' if data['contract_type'] == CONTRACT_TYPE_HPHC else 'tempo'
            tarif_key = contract_type_key+'_variable_hp_ttc'
            for range in off_peak_hours_ranges.split(','):
                if not re.match(r'([0-1]?[0-9]|2[0-3]):[0-5][0-9]-([0-1]?[0-9]|2[0-3]):[0-5][0-9]', range):
                    continue

                hours = range.split('-')
                start_at = str_to_time(hours[0])
                end_at = str_to_time(hours[1])

                if time_in_between(now.time(), start_at, end_at):
                    tarif_key = contract_type_key+'_variable_hc_ttc'
                    break

            if tarif_key in self.data:
                self.data['tarif_actuel_ttc'] = self.data[tarif_key]

        self.logger.debug('EDF Tarif')
        self.logger.debug(self.data)

        return self.data
