"""TC20E alarm coordinator."""

from __future__ import annotations

import asyncio
from datetime import timedelta
import re

import aiohttp
from bs4 import BeautifulSoup

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_AUTHENTICATION
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .const import DOMAIN, LOGGER, MIN_SCAN_INTERVAL, TC20E_URL

# Global timeout for HTTP calls to TC20E.
# The backend is known to be very slow, so keep this relatively high
# to reduce spurious timeouts while still failing eventually.
TIMEOUT = 30


class TC20EUpdateCoordinator(DataUpdateCoordinator):
    """TC20E Coordinator."""

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        """Initialize the TC20E Coordinator."""
        self.websession = async_get_clientsession(hass)
        self._authid: str = entry.data[CONF_AUTHENTICATION]
        self._session_id: str | None = None

        self._timesync = MIN_SCAN_INTERVAL
        self.alarmstatus = 0

        # Serialize all requests to TC20E to avoid concurrent sessions and deadlocks.
        self._request_lock = asyncio.Lock()

        super().__init__(
            hass,
            LOGGER,
            name=DOMAIN,
            update_interval=timedelta(seconds=MIN_SCAN_INTERVAL),
        )

    async def setalarm(self, command: str) -> None:
        """Change status of alarm."""
        try:
            if command == "full":
                await self._request(
                    TC20E_URL + "/applicationservice/domoweb/panel/commands/arm"
                )
                self.alarmstatus = 101

            if command == "partial":
                await self._request(
                    TC20E_URL + "/applicationservice/domoweb/panel/commands/partialarm"
                )
                self.alarmstatus = 102

            if command == "disarm":
                await self._request(
                    TC20E_URL + "/applicationservice/domoweb/panel/commands/disarm"
                )
                self.alarmstatus = 100

        except (UpdateFailed, CannotConnectError, AuthenticationError) as error:
            raise HomeAssistantError(
                f"Could not arm/disarm TC20E on error {error!s}"
            ) from error

        # await self.async_request_refresh()

    async def _async_update_data(self) -> None:
        """Fetch info from TC20E."""
        LOGGER.debug("Trying to get Alarm status")

        try:
            await self._request(
                TC20E_URL + "/applicationservice/domoweb/panel/commands/status"
            )
        except (UpdateFailed, CannotConnectError, AuthenticationError) as error:
            raise HomeAssistantError(
                f"Could not retrieve alarm status on error {error!s}"
            ) from error

        # await self.async_request_refresh()

    async def _request(self, url: str) -> None:
        """Perform a serialized request to TC20E and update alarmstatus.

        This method must never leave the coordinator in a locked/broken state.
        Any network/DNS error should fail the current update, but the next update
        must be able to run normally without requiring an integration reload.
        """
        async with self._request_lock:
            try:
                # Login and retrieve a session token.
                try:
                    async with asyncio.timeout(TIMEOUT):
                        await self._login()
                except TimeoutError as error:
                    LOGGER.warning("Timeout during login: %s", str(error))
                    raise CannotConnectError from error

                LOGGER.debug("Login passed")

                headers = {
                    "x-session-token": self._session_id,
                }
                params = {
                    "isBusy": "true",
                    "checkCompletion": "true",
                }
                json_payload = {
                    "key": "",
                    "value": "",
                }

                # Send the command/status request.
                try:
                    async with asyncio.timeout(TIMEOUT):
                        response = await self.websession.put(
                            url, headers=headers, params=params, json=json_payload
                        )
                except TimeoutError as error:
                    LOGGER.warning("Timeout when sending command to TC20E")
                    raise CannotConnectError from error
                except Exception as error:
                    LOGGER.debug("Exception on request: %s", error)
                    raise UpdateFailed from error

                LOGGER.debug("Command response status: %s", response.status)

                if response.status == 200:
                    try:
                        data = await response.json()
                        json_id = data["id"]
                        json_status = data["status"]
                    except aiohttp.ContentTypeError as error:
                        LOGGER.debug("ContentTypeError on ok status: %s", error.message)
                        response_text = await response.text()
                        LOGGER.debug("Response (200) text is: %s", response_text)
                        raise UpdateFailed from error

                    if json_status == "success":
                        LOGGER.debug("Command successful, URL: %s", url)

                        statuscode = 0
                        messagekey = None
                        errorcode = None

                        # Poll the status endpoint until completion.
                        while statuscode != 2:
                            try:
                                async with asyncio.timeout(TIMEOUT):
                                    response = await self.websession.get(
                                        f"{url}/{json_id}/status",
                                        headers=headers,
                                    )
                            except TimeoutError as error:
                                LOGGER.warning("Timeout while polling TC20E status")
                                raise CannotConnectError from error
                            except Exception as error:
                                LOGGER.debug("Exception on request: %s", error)
                                raise UpdateFailed from error

                            if response.status == 200:
                                LOGGER.debug(
                                    "Command poll response status: %s", response.status
                                )
                                try:
                                    data = await response.json()
                                    statuscode = data["statusCode"]
                                    messagekey = data.get("messageKey")
                                    errorcode = data.get("errorCode")
                                except aiohttp.ContentTypeError as error:
                                    LOGGER.debug(
                                        "ContentTypeError on ok status: %s",
                                        error.message,
                                    )
                                    response_text = await response.text()
                                    LOGGER.debug("Response (200) text is: %s", response_text)
                                    raise UpdateFailed from error

                                LOGGER.debug("Command response Status Code: %s", statuscode)

                            await asyncio.sleep(1)

                            # Code 6 is "too long" in the original implementation.
                            if statuscode == 6:
                                LOGGER.debug("Status code is 6 -> Too long, aborting")
                                self.alarmstatus = 0
                                raise UpdateFailed

                        LOGGER.debug("Status Code is: %s", statuscode)
                        LOGGER.debug("Error Code is: %s", errorcode)
                        LOGGER.debug("Message is: %s", messagekey)

                        if errorcode is not None:
                            self.alarmstatus = errorcode

                        return

                if response.status == 201:
                    try:
                        data = await response.json()
                        statuscode = data["statusCode"]
                        messagekey = data.get("messageKey")
                        errorcode = data.get("errorCode")
                    except aiohttp.ContentTypeError as error:
                        LOGGER.debug("ContentTypeError on ok status: %s", error.message)
                        response_text = await response.text()
                        LOGGER.debug("Response (201) text is: %s", response_text)
                        raise UpdateFailed from error

                    if statuscode == 6:
                        LOGGER.debug("Status code is 6 -> Too long, aborting")
                        self.alarmstatus = 0
                        raise UpdateFailed

                    LOGGER.debug("Status Code is: %s", statuscode)
                    LOGGER.debug("Error Code is: %s", errorcode)
                    LOGGER.debug("Message is: %s", messagekey)

                    if errorcode is not None:
                        self.alarmstatus = errorcode

                    return

                LOGGER.debug("Did not retrieve information properly")
                LOGGER.debug("request status: %s", response.status)
                response_text = await response.text()
                LOGGER.debug("request text: %s", response_text)
                raise UpdateFailed

            finally:
                # Always cleanup local session state. This must never block the lock release.
                await self._logout()

    async def _logout(self) -> None:
        """Logout and always clear local session state.

        Network/DNS issues are common with this backend; logout failures must not
        prevent future updates.
        """
        LOGGER.debug("Logout")

        try:
            async with asyncio.timeout(TIMEOUT):
                await self.websession.get(
                    f"{TC20E_URL}/logout",
                    headers={
                        "Connection": "keep-alive",
                    },
                )
        except Exception as err:
            # Logout errors should not block the coordinator.
            LOGGER.debug("Logout failed (ignored): %s", err)
        finally:
            self._session_id = None

    async def _login(self) -> None:
        """Login and retrieve session id."""
        LOGGER.debug("Trying to login")

        async with asyncio.timeout(TIMEOUT):
            await self.websession.get(TC20E_URL)

            response = await self.websession.get(
                f"{TC20E_URL}/validate",
                headers={
                    "Authorization": self._authid,
                },
            )

        response_text = await response.text()

        if response_text != "#1home":
            LOGGER.error("Auth failure %s, status %s", response_text, response.status)
            self._session_id = None
            raise AuthenticationError

        response = await self.websession.get(
            f"{TC20E_URL}/go/home",
            headers={
                "Connection": "keep-alive",
            },
        )
        response_text = await response.text()
        soup = BeautifulSoup(response_text, "html.parser")

        try:
            self._session_id = re.search(
                r"homeSessionId='(.*?)'", soup.prettify()
            ).group(1)
        except AttributeError as err:
            LOGGER.error("Failed to retrieve Session ID: %d", response.status)
            # Do not call _logout() here; _request() already guarantees cleanup in finally.
            self._session_id = None
            raise CannotConnectError from err

        if self._session_id is None:
            LOGGER.error("Failed to retrieve Session ID: %d", response.status)
            raise CannotConnectError

        LOGGER.debug("Session id retrieved")


class UnauthorizedError(HomeAssistantError):
    """Exception to indicate an error in authorization."""


class CannotConnectError(HomeAssistantError):
    """Exception to indicate an error in client connection."""


class OperationError(HomeAssistantError):
    """Exception to indicate an error in operation."""


class AuthenticationError(HomeAssistantError):
    """Error to indicate authentication failure."""
