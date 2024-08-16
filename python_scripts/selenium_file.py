"""!
@author: Saurabh Baid
@date: May-2016
@contact: saurabh.baid@cambiumnetworks.com
@summary: This library provides API to interact with CNMaestro using different web browsers. The library is supposed to be x-platoform and if there
any limitation it would be mentioned in the individual APIs.  Currently following WebBrowsers are supported.
 - Firefox
 - Chrome
 - Safari (On MAC)
 - Internet Explorer (Windows 7)
 - Edge (Windows 10)
"""
import re
import logging
import sys
import os
import time
import subprocess

from selenium import webdriver
from selenium.webdriver.support.events import EventFiringWebDriver
from selenium.webdriver.support.events import AbstractEventListener
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.remote.command import Command
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.remote_connection import LOGGER

from utils.host_utils import get_temp_dir


log = logging.getLogger()
# logging.basicConfig(level='INFO')

# Below code is currently creating problem while running through jenkins
# I think win32api is not in python path but need to be investigated further
# if sys.platform == 'win32':
#     import win32api
#     win32api.SetCursorPos((0,0))
#
# log = logging.getLogger()


def get_download_dir():
    return get_temp_dir()


class ScreenShotListener(AbstractEventListener):
    DIR_NAME = None

    @staticmethod
    def screen_shot(driver):
        if ScreenShotListener.DIR_NAME is None:
            dir_name = os.path.curdir
        else:
            dir_name = ScreenShotListener.DIR_NAME

        unique_filename = str(int(time.time() * 1000)) + ".png"
        fpath = os.path.join(dir_name, unique_filename)
        try:
            driver.get_screenshot_as_file(fpath)
            # log.debug("Screenshot " + os.path.realpath(fpath))
        except BaseException:
            log.warning("Unable to take screen shot, %s" % sys.exc_info()[1])

    def before_find(self, by, value, driver):
        self.screen_shot(driver)

    def before_click(self, element, driver):
        try:
            self._parent.execute_script("arguments[0].scrollIntoView();", self)
        except BaseException:
            pass
        self.screen_shot(driver)

    def after_click(self, element, driver):
        self.screen_shot(driver)

    def before_close(self, driver): self.screen_shot(driver)

    def before_quit(self, driver): self.screen_shot(driver)

    def on_exception(self, exception, driver): self.screen_shot(driver)

"""
@summary: This API is to launch chrome webbrowser.
@requires: It requires chrome and chrome driver to be installed. Instructions are here https://christopher.su/2015/selenium-chromedriver-ubuntu/
@return: <Selenium WebDriver object.>
"""


def _launch_chrome():
    global download_dir
    # dc = webdriver.DesiredCapabilities.CHROME
    # dc["loggingPrefs"] = {"performance": "ALL"}
    log.debug("Setting Chrome Options")
    chrome_profile = webdriver.ChromeOptions()
    profile = {"download.default_directory": get_download_dir(),
               "download.prompt_for_download": False,
               "download.directory_upgrade": True,
               }
    chrome_profile.add_argument("--no-sandbox")
    chrome_profile.add_argument('--disable-dev-shm-usage')
    chrome_profile.add_argument('--ignore-certificate-errors')
    chrome_profile.add_argument("--disable-extensions")
    chrome_profile.add_argument("--disable-gpu")
    chrome_profile.add_experimental_option("prefs", profile)
    LOGGER.setLevel(logging.WARNING)
    log.debug("Launching Chrome")
    driver = webdriver.Chrome(
        options=chrome_profile
        # ,
        # desired_capabilities=dc
    )
    driver = EventFiringWebDriver(driver, ScreenShotListener())
    driver.maximize_window()
    return driver


desired_capabilities = {'aut': 'io.selendroid.testapp:0.10.0'}


def _launch_android():
    # desired_capabilities = {'aut': 'io.selendroid.androiddriver:0.17.0'}
    # driver = webdriver.Remote(desired_capabilities=webdriver.DesiredCapabilities.ANDROID)
    log.info("launching chrome on android device")
    capabilities = {
        'chromeOptions': {
            'androidPackage': 'com.android.chrome',

        }
    }
    chrome_profile = webdriver.ChromeOptions()
    profile = {
        'credentials_enable_service': False,
        'profile': {
            'password_manager_enabled': False
        }
    }
    chrome_profile.add_experimental_option('prefs', profile)
    log.info("Verifying that android device is connected via USB")
    enable_disable_android_input_methods("disable")

    p = subprocess.Popen("adb shell ime list".split(), stdout=subprocess.PIPE)
    line = p.stdout.readline()
    while line:
        m = re.search("mId=(.*)", line)
        if m:
            log.info("Disabling Keyboard layout: %s" % line)
            cmd = "adb shell ime disable %s" % m.group(1)
            q = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
            out, err = q.communicate()
            log.info(out)
        line = p.stdout.readline()

    driver = webdriver.Chrome(desired_capabilities=capabilities)
    driver = EventFiringWebDriver(driver, ScreenShotListener())
    return driver


def enable_disable_android_input_methods(action):
    p = subprocess.Popen(["adb", "devices"], stdout=subprocess.PIPE)
    line = p.stdout.readline()
    while line:
        log.info(line)
        if re.match(r"\S+\s+device", line):
            break
        line = p.stdout.readline()
    else:
        raise AssertionError("Device not connected via USB")
    p = subprocess.Popen(
        "adb shell ime list -a".split(),
        stdout=subprocess.PIPE)
    line = p.stdout.readline()
    while line:
        m = re.search("mId=(.*)", line)
        if m:
            if action.lower() == 'enable':
                log.info("Enabling Keyboard layout: %s" % line)
                cmd = "adb shell ime enable %s" % m.group(1)
            else:
                log.info("Disabling Keyboard layout: %s" % line)
                cmd = "adb shell ime disable %s" % m.group(1)
            q = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
            out, err = q.communicate()
            log.info(out)
        line = p.stdout.readline()


def _launch_chrome_emulator():
    global download_dir
    mobile_emulation = {
        "deviceName": "Google Nexus 5"
    }
    dc = webdriver.DesiredCapabilities.CHROME
    log.debug("Setting Chrome Options")
    chrome_profile = webdriver.ChromeOptions()
    profile = {"download.default_directory": get_download_dir(),
               "download.prompt_for_download": False,
               "download.directory_upgrade": True,
               }
    chrome_profile.add_argument('--ignore-certificate-errors')
    chrome_profile.add_argument("--disable-extensions")
    chrome_profile.add_experimental_option("prefs", profile)
    chrome_profile.add_experimental_option("mobileEmulation", mobile_emulation)
    log.debug("Launching Chrome")
    driver = webdriver.Chrome(
        chrome_options=chrome_profile,
        desired_capabilities=dc)
    driver = EventFiringWebDriver(driver, ScreenShotListener())
    return driver


"""!
@summary: This API is to launch Fire Fox web browser.
@return: <Selenium WebDriver object.>
"""


def _launch_firefox():
    global download_dir
    dc = webdriver.DesiredCapabilities.FIREFOX
    dc['nativeEvents'] = False
    log.debug("Setting up firefox profile")
    profile = webdriver.FirefoxProfile()
    profile.set_preference('browser.download.folderList', 2)
    profile.set_preference('browser.download.manager.showWhenStarting', False)
    profile.set_preference('browser.download.dir', get_download_dir())
    profile.set_preference(
        'browser.helperApps.neverAsk.saveToDisk',
        'application/x-gzip')
    log.debug("Launching Firefox")
    driver = webdriver.Firefox(profile, capabilities=dc)
    driver.maximize_window()
    return driver


"""!
@summary: This API is to launch Safari web browser.
@return: <Selenium WebDriver object.>
"""


def _launch_safari():
    log.debug("Launching Safari")
    capabilities = webdriver.DesiredCapabilities.SAFARI
    capabilities["acceptInsecureCerts"] = True
    driver = webdriver.Safari(desired_capabilities=capabilities)
    driver = EventFiringWebDriver(driver, ScreenShotListener())
    driver.maximize_window()
    return driver


"""!
@summary: This API is to launch Internet Explorer web browser.
@return: <Selenium WebDriver object.>
"""


def _launch_ie():
    dc = webdriver.DesiredCapabilities.INTERNETEXPLORER
    dc['nativeEvents'] = False
    log.debug("Launching Internet Explorer")
    driver = webdriver.Ie(capabilities=dc)
    driver.maximize_window()
    return driver


"""!
@summary: This API is to launch Edge web browser.
@return: <Selenium WebDriver object.>
"""


def _launch_edge():
    dc = webdriver.DesiredCapabilities.EDGE
    dc["enablePersistentHovering"] = False
    dc['nativeEvents'] = False
    log.debug("Launching Edge Browser")
    driver = webdriver.Edge(capabilities=dc)
    driver.maximize_window()
    return driver


def _click(self):
    try:
        if self.parent.name == 'chrome':
            self._execute(Command.CLICK_ELEMENT)
        else:
            self._parent.execute_script("arguments[0].click()", self)
    except BaseException:
        try:
            ActionChains(self._parent).move_to_element(self).click().perform()
        except BaseException:
            self._parent.execute_script("arguments[0].click()", self)


def _set_text(self, text):
    # It will do 3 retries to set the text,
    # After each try it will see if the value is as per expected text and it will quit
    # if value is not same after 3 tries and raises exception
    retry_count = 0
    MAX_RETRY = 3
    text = str(text)
    while retry_count < MAX_RETRY:
        self.clear()
        self.send_keys(text)
        time.sleep(1)
        if self.get_attribute('value') == text:
            break
        else:
            retry_count += 1
    else:
        raise ValueError("Not able to set text on " + str(self))


def _get_text(self):
    try:
        self._parent.execute_script("arguments[0].scrollIntoView();", self)
    except BaseException:
        pass
    return self.text


def _check(self):
    if self.get_attribute("checked") is None or self.get_attribute(
            "checked") != 'true':
        self.click()


def _uncheck(self):
    if self.get_attribute("checked") is not None and self.get_attribute(
            "checked") == 'true':
        self.click()


def _get_attr(self, name):
    try:
        self._parent.execute_script("arguments[0].scrollIntoView();", self)
    except BaseException:
        log.error("Could not scroll element into view")
    return self.__getattribute__(name)


def _find_elem(
        self,
        by=By.ID or By.XPATH or By.LINK_TEXT or By.NAME or By.TAG_NAME or By.CLASS_NAME,
        value=None):

    elem = self.find_element_orig(by, value)

    try:
        orig_style = elem.get_attribute('style')
        change_style = orig_style + "border: 2px solid red;"
        self.execute_script(
            "arguments[0].setAttribute('style', arguments[1])",
            elem,
            change_style)
        self._parent.execute_script("arguments[0].scrollIntoView();", self)
    except Exception:
        pass
    return elem


def _action_chains_move_to_element(self, to_element):
    """
    Overrides ActionsChains.move_to_element method.move_t0_element accepts WebElement
    But in Framework we always have EventFiringWebelement (Wrapper on web element).
    So to handle the here we are overriding move_to_element method.
    """
    # Check if given element is web element or EventFiring webelement.
    # If it is event firing element, reads web element from it.
    if isinstance(to_element,
                  webdriver.support.event_firing_webdriver.EventFiringWebElement):
        to_element = to_element._webelement

    if self._driver.w3c:
        self.w3c_actions.pointer_action.move_to(to_element)
        self.w3c_actions.key_action.pause()
    else:
        self._actions.append(lambda: self._driver.execute(
            Command.MOVE_TO, {'element': to_element.id}))
    return self


EventFiringWebDriver.find_element_orig = EventFiringWebDriver.find_element
EventFiringWebDriver.find_element = _find_elem
WebElement.set_text = _set_text
WebElement.click = _click
WebElement.check = _check
WebElement.uncheck = _uncheck
WebElement.get_text = _get_text
ActionChains.move_to_element = _action_chains_move_to_element
# WebElement.__getattribute__orig = WebElement.__getattribute__
WebElement.__getattr__ = _get_attr
WebElement._getattr__ = _get_attr


if __name__ == "__main__":
    a = _launch_chrome()
    a.get("https://www.google.com")
    ActionChains(a).move_to_element("//div[@class='FPdoLc lJ9FBc']//input[@name='btnK']").perform()
    # _action_chains_move_to_element(a, "//div[@class='FPdoLc lJ9FBc']//input[@name='btnK']")
    time.sleep(5)
    a.quit()
