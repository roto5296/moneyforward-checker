import chromedriver_binary
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.action_chains import ActionChains
import re
import json
import requests
import html


class MoneyForward:
    def __init__(self, jsontext):
        tmp = json.loads(jsontext)
        self._id = tmp['id']
        self._pass = tmp['pass']
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument(
            '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Safari/537.36')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        print('connecting to remote browser...')
        self._driver = webdriver.Chrome(options=options)
        self._driver.implicitly_wait(5)

    def login(self):
        try:
            self._driver.get('https://moneyforward.com')
            print('login...')
            self._driver.find_element(
                By.XPATH, '//p[@class="web-sign-in"]/a').click()  # click login
            self._driver.find_element(
                By.XPATH, '//a[@class="_2YH0UDm8 ssoLink"]').click()  # click e-mail login
            self._driver.find_element(
                By.XPATH, '//input[@class="_2mGdHllU inputItem "]').send_keys(self._id)  # input e-mail address
            self._driver.find_element(
                By.XPATH, '//input[@class="zNNfb322 submitBtn homeDomain"]').click()  # click login
            self._driver.find_element(
                By.XPATH, '//input[@class="_1vBc2gjI inputItem "]').send_keys(self._pass)  # input PW
            current_url = self._driver.current_url
            self._driver.find_element(
                By.XPATH, '//input[@class="zNNfb322 submitBtn homeDomain"]').click()  # click login
            WebDriverWait(self._driver, 10).until(
                EC.url_changes(current_url))  # wait login
            print("LOGIN SUCCESS")
            return True
        except TimeoutException:
            print("LOGIN TIMEOUT!!")
            return False

    def update(self):
        try:
            print('update...')
            updates = self._driver.find_elements(
                By.XPATH, '//li[contains(@class,"controls")]/a[contains(text(),"更新")]')  # get update button
            for update in updates:
                update.click()  # click update
            WebDriverWait(self._driver, 300).until(EC.invisibility_of_element_located(
                (By.XPATH, '//li[contains(@class,"loding")]')))  # wait for update
            print("UPDATE SUCCESS")
            return True
        except TimeoutException:
            print("UPDATE TIMEOUT!!")
            return False

    def get(self, year, month, use_selenium=False):
        if use_selenium:
            try:
                actions = ActionChains(self._driver)
                if self._driver.current_url != 'https://moneyforward.com/cf':
                    self._driver.find_element(
                        By.XPATH, '//li[contains(descendant::text(), "家計")]').click()  # click kakei
                xpath = '//span[@class="uikit-year-month-select-dropdown-text"]'
                actions.move_to_element(
                    self._driver.find_element(By.XPATH, xpath)).perform()
                # click year month select
                self._driver.find_element(By.XPATH, xpath).click()
                xpath = '//div[@class="uikit-year-month-select-dropdown-year-part" and contains(text(), "' + str(
                    year) + '")]'
                actions.move_to_element(self._driver.find_element(
                    By.XPATH, xpath)).perform()  # mouse over year
                xpath = '//a[@data-month="' + \
                    str(1) + '" and @data-year="' + str(year) + '"]'
                actions.move_to_element(self._driver.find_element(
                    By.XPATH, xpath)).perform()  # mouse over year
                xpath = '//a[@data-month="' + \
                    str(month) + '" and @data-year="' + str(year) + '"]'
                self._driver.find_element(
                    By.XPATH, xpath).click()  # click month
                xpath = '//span[@class="fc-header-title in-out-header-title fc-state-disabled"]'
                WebDriverWait(self._driver, 3).until(
                    EC.presence_of_all_elements_located((By.XPATH, xpath)))  # wait loading
                xpath = '//span[@class="fc-header-title in-out-header-title"]'
                WebDriverWait(self._driver, 3).until(
                    EC.presence_of_all_elements_located((By.XPATH, xpath)))  # wait loading
                elements = self._driver.find_elements(
                    By.XPATH, '//table[@id="cf-detail-table"]/tbody/tr')  # get table
                ret = []
                for element in elements:
                    tds = element.find_elements(By.XPATH, './td')
                    try:
                        tmp = WebDriverWait(self._driver, 0.1).until(
                            lambda d: tds[0].find_element(By.XPATH, './/i'))
                        if 'icon-ban-circle' in tmp.get_attribute("class"):
                            continue
                    except:
                        pass
                    ret.append([td.text.replace('\n', '') for td in tds])
            except TimeoutException:
                print("TIMEOUT!!")
                ret = []
        else:
            csrf = self._driver.find_element(
                By.XPATH, '//meta[@name="csrf-token"]').get_attribute("content")
            headers = {"Accept": "text/javascript",
                       "X-CSRF-Token": csrf, "X-Requested-With": "XMLHttpRequest"}
            session = requests.session()
            for cookie in self._driver.get_cookies():
                session.cookies.set(cookie["name"], cookie["value"])
            post_data = "from="+str(year)+"/"+str(month) + \
                "/1&service_id=&account_id_hash="
            result = session.post(
                "https://moneyforward.com/cf/fetch", data=post_data, headers=headers)
            tmp = re.search(r'\$\("\.list_body"\)\.append\(\'(.*?)\'\);', html.unescape(
                result.text)).group(1).replace(r'\n', '').replace('\\', '')
            trs = re.findall(r'<tr.*?<\/tr>', tmp)
            ret = []
            for tr in trs:
                rets = []
                if 'icon-ban-circle' in tr:
                    continue
                rets.append(
                    re.search(r'data-table-sortable-value=\'(.*?)\'>', tr).group(1))
                for tds in re.findall(r'<td.*?<\/td>', tr):
                    tds = re.sub(r'<select.*>.*</select.*>', '', tds)
                    tds = re.sub(r'<.*?>', '', tds)
                    rets.append(tds)
                ret.append(rets)
            ret.sort(reverse=True)
            ret = [i[1:] for i in ret]
        return self._convert_mfdata(ret, year)

    def _convert_mfdata(self, text_data, year):
        ret = []
        for tds in text_data:
            tmp = tds[1]
            date = str(year)+"-"+tmp[0:2]+"-"+tmp[3:5]
            content = tds[2]
            tmp = tds[3]
            if "振替" in tmp:
                furikae = True
                price = int(re.sub('[^0-9]', '', tmp))
            else:
                furikae = False
                price = int(re.sub('[^0-9-]', '', tmp))
            bank = tds[4]
            item1 = "振替" if furikae else tds[5]
            item2 = tds[6]
            memo = tds[7]
            ret.append([date, content, price, bank, item1, item2, memo])
        return ret

    def __del__(self):
        self._driver.quit()
