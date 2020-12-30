from abc import ABCMeta, abstractmethod
import chromedriver_binary  # noqa
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
import urllib
import time
from bs4 import BeautifulSoup as BS


class MoneyForwardABC(metaclass=ABCMeta):
    @abstractmethod
    def __init__(self, jsontext):
        tmp = json.loads(jsontext)
        self._id = tmp['id']
        self._pass = tmp['pass']

    def login(self):
        print('login...')
        if self._inner_login():
            print("LOGIN SUCCESS")
            return True
        else:
            print("LOGIN FAIL")
            return False

    def update(self):
        print('update...')
        if self._inner_update():
            print("UPDATE SUCCESS")
            return True
        else:
            print("UPDATE FAIL")
            return False

    def get(self, year, month):
        print('get...')
        ret = self._inner_get(year, month)
        return self._convert_mfdata(ret, year)

    @abstractmethod
    def _inner_login(self):
        pass

    @abstractmethod
    def _inner_update(self):
        pass

    @abstractmethod
    def _inner_get(self, year, month):
        pass

    def _convert_mfdata(self, dataset, year):
        ret = []
        for data in dataset:
            (transaction_id, tds) = data
            tmp = tds[1]
            date = str(year) + "-" + tmp[0:2] + "-" + tmp[3:5]
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
            ret.append([
                transaction_id, date, content,
                price, bank, item1, item2, memo
            ])
        ret = sorted(ret, key=lambda x: (x[1], x[0]), reverse=True)
        return ret


class MoneyForwardSelenium(MoneyForwardABC):
    def __init__(self, jsontext):
        super().__init__(jsontext)
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument(
            ('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
             ' AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.66'
             ' Safari/537.36')
        )
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        print('connecting to remote browser...')
        self._driver = webdriver.Chrome(options=options)
        self._driver.implicitly_wait(5)

    def _inner_login(self):
        try:
            self._driver.get('https://moneyforward.com')
            # click login
            self._driver.find_element(
                By.XPATH, '//p[@class="web-sign-in"]/a'
            ).click()
            # click e-mail login
            self._driver.find_element(
                By.XPATH, '//a[@class="_2YH0UDm8 ssoLink"]'
            ).click()
            # input e-mail address
            self._driver.find_element(
                By.XPATH, '//input[@class="_2mGdHllU inputItem "]'
            ).send_keys(self._id)
            # click login
            self._driver.find_element(
                By.XPATH, '//input[@class="zNNfb322 submitBtn homeDomain"]'
            ).click()
            # input PW
            self._driver.find_element(
                By.XPATH, '//input[@class="_1vBc2gjI inputItem "]'
            ).send_keys(self._pass)
            current_url = self._driver.current_url
            # click login
            self._driver.find_element(
                By.XPATH, '//input[@class="zNNfb322 submitBtn homeDomain"]'
            ).click()
            # wait login
            WebDriverWait(self._driver, 10).until(
                EC.url_changes(current_url)
            )
            return True
        except TimeoutException:
            return False

    def _inner_update(self):
        try:
            # get update button
            updates = self._driver.find_elements(
                By.XPATH,
                '//li[contains(@class,"controls")]/a[contains(text(),"更新")]'
            )
            for update in updates:
                # click update
                update.click()
            # wait for update
            WebDriverWait(self._driver, 300).until(
                EC.invisibility_of_element_located(
                    (By.XPATH, '//li[contains(@class,"loding")]')
                )
            )
            return True
        except TimeoutException:
            return False

    def _inner_get(self, year, month):
        try:
            actions = ActionChains(self._driver)
            if self._driver.current_url != 'https://moneyforward.com/cf':
                # click kakei
                self._driver.find_element(
                    By.XPATH, '//li[contains(descendant::text(), "家計")]'
                ).click()
            xpath = '//span[@class="uikit-year-month-select-dropdown-text"]'
            actions.move_to_element(
                self._driver.find_element(By.XPATH, xpath)
            ).perform()
            # click year month select
            self._driver.find_element(By.XPATH, xpath).click()
            xpath = (
                '//div[@class="uikit-year-month-select-dropdown-year-part"'
                ' and contains(text(), "'
            ) + str(year) + '")]'
            # mouse over year
            actions.move_to_element(self._driver.find_element(
                By.XPATH, xpath
            )).perform()
            xpath = '//a[@data-month="' + \
                str(1) + '" and @data-year="' + str(year) + '"]'
            # mouse over year
            actions.move_to_element(self._driver.find_element(
                By.XPATH, xpath
            )).perform()
            xpath = '//a[@data-month="' + \
                str(month) + '" and @data-year="' + str(year) + '"]'
            # click month
            self._driver.find_element(
                By.XPATH, xpath
            ).click()
            xpath = (
                '//span[@class="fc-header-title in-out-header-title'
                ' fc-state-disabled"]'
            )
            # wait loading
            WebDriverWait(
                self._driver, 3
            ).until(EC.presence_of_all_elements_located((By.XPATH, xpath)))
            xpath = '//span[@class="fc-header-title in-out-header-title"]'
            # wait loading
            WebDriverWait(self._driver, 3).until(
                EC.presence_of_all_elements_located((By.XPATH, xpath))
            )
            # get table
            elements = self._driver.find_elements(
                By.XPATH, '//table[@id="cf-detail-table"]/tbody/tr'
            )
            ret = []
            for element in elements:
                transaction_id = int(
                    element.get_attribute("id").replace(
                        'js-transaction-', ''
                    )
                )
                tds = element.find_elements(By.XPATH, './td')
                try:
                    tmp = WebDriverWait(self._driver, 0.1).until(
                        lambda d: tds[0].find_element(By.XPATH, './/i')
                    )
                    if 'icon-ban-circle' in tmp.get_attribute("class"):
                        continue
                except BaseException:
                    pass
                ret.append(
                    (transaction_id, [td.text.replace('\n', '') for td in tds])
                )
        except TimeoutException:
            ret = []
        return ret

    def __del__(self):
        self._driver.quit()


class MoneyForwardRequests(MoneyForwardABC):
    def __init__(self, jsontext):
        super().__init__(jsontext)
        self._session = requests.session()

    def _inner_login(self, use_selenium=False):
        result = self._session.get("https://moneyforward.com/sign_in/")
        qs = urllib.parse.urlparse(result.url).query
        qs_d = urllib.parse.parse_qs(qs)
        token = re.search(
            r'<meta name="csrf-token" content="(.*?)" \/>',
            html.unescape(result.text)
        ).group(1)
        post_data = {
            "authenticity_token": token,
            "_method": "post",
            "mfid_user[email]": self._id,
            "mfid_user[password]": self._pass,
            "select_account": "true"
        }
        post_data.update(qs_d)
        result = self._session.post(
            "https://id.moneyforward.com/sign_in", data=post_data
        )
        if result.url == "https://moneyforward.com/" \
                and result.status_code == 200:
            return True
        else:
            return False

    def _inner_update(self):
        result = self._session.get("https://moneyforward.com")
        pattern = re.compile(
            (r'<a data-remote="true" rel="nofollow"'
             ' data-method="post" href="(.*?)">')
        )
        urls = re.findall(pattern, html.unescape(result.text))
        token = re.search(
            r'<meta name="csrf-token" content="(.*?)" \/>',
            html.unescape(result.text)
        ).group(1)
        headers = {
            "Accept": "text/javascript",
            "X-CSRF-Token": token,
            "X-Requested-With": "XMLHttpRequest"
        }
        self._results = []
        for url in urls:
            self._session.post(
                "https://moneyforward.com" + url,
                headers=headers
            )
        delay = 2
        counter = 0
        while counter < 300:
            time.sleep(delay)
            counter += delay
            result = self._session.get(
                "https://moneyforward.com/accounts/polling"
            )
            if not result.json()["loading"]:
                return True
        return False

    def _inner_get(self, year, month):
        result = self._session.get("https://moneyforward.com")
        token = re.search(
            r'<meta name="csrf-token" content="(.*?)" \/>',
            html.unescape(result.text)
        ).group(1)
        headers = {
            "Accept": "text/javascript",
            "X-CSRF-Token": token,
            "X-Requested-With": "XMLHttpRequest"
        }
        post_data = {
            "from": str(year) + "/" + str(month) + "/1",
            "service_id": "",
            "account_id_hash": "",
        }
        result = self._session.post(
            "https://moneyforward.com/cf/fetch",
            data=post_data,
            headers=headers
        )
        tmp = re.search(
            r'\$\("\.list_body"\)\.append\(\'(.*?)\'\);',
            html.unescape(result.text)
        ).group(1).replace(r'\n', '').replace('\\', '')
        trs = re.findall(r'<tr.*?<\/tr>', tmp)
        ret = []
        for tr in trs:
            rets = []
            if 'icon-ban-circle' in tr:
                continue
            transaction_id = int(re.search(
                r'id=\'js-transaction-(.*?)\'>', tr
            ).group(1))
            for tds in re.findall(r'<td.*?<\/td>', tr):
                tds = re.sub(r'<select.*>.*</select.*>', '', tds)
                tds = re.sub(r'<.*?>', '', tds)
                rets.append(tds)
            ret.append((transaction_id, rets))
        return ret

    def insert(
            self, year, month, day, price, account,
            l_category='未分類', m_category='未分類', memo=''
    ):
        result = self._session.get('https://moneyforward.com/cf')
        soup = BS(result.content, 'html.parser')
        categories = {}
        find_classes = [
            'dropdown-menu main_menu plus',
            'dropdown-menu main_menu minus'
        ]
        keys = ['plus', 'minus']
        for (find_class, key) in zip(find_classes, keys):
            d_pm = {}
            c_pm = soup.find('ul', class_=find_class)
            for l_c in c_pm.find_all('li', class_='dropdown-submenu'):
                d = {m_c.text: {'id': int(m_c['id'])}
                     for m_c in l_c.find_all('a', class_='m_c_name')}
                tmp = l_c.find('a', class_='l_c_name')
                d.update({'id': int(tmp['id'])})
                d_pm.update({tmp.text: d})
            categories.update({key: d_pm})
        tmp = soup.find('select', id='user_asset_act_sub_account_id_hash')
        accounts = {}
        for ac in tmp.find_all('option'):
            accounts.update({ac.text.split()[0]: ac['value']})
        try:
            if price > 0:
                is_income = 1
                l_c_id = categories['plus'][l_category]['id']
                m_c_id = categories['plus'][l_category][m_category]['id']
            else:
                is_income = 0
                l_c_id = categories['minus'][l_category]['id']
                m_c_id = categories['minus'][l_category][m_category]['id']
            account_id = accounts[account]
        except BaseException:
            return False

        token = re.search(
            r'<meta name="csrf-token" content="(.*?)" \/>',
            html.unescape(result.text)
        ).group(1)
        headers = {
            "Accept": "text/javascript",
            "X-CSRF-Token": token,
            "X-Requested-With": "XMLHttpRequest"
        }
        date = str(year) + '/' + str(month).zfill(2) + '/' + str(day).zfill(2)
        post_data = {
            'user_asset_act[is_transfer]': 0,
            'user_asset_act[is_income]': is_income,
            'user_asset_act[updated_at]': date,
            'user_asset_act[recurring_flag]': 0,
            'user_asset_act[amount]': abs(price),
            'user_asset_act[sub_account_id_hash]': account_id,
            'user_asset_act[large_category_id]': l_c_id,
            'user_asset_act[middle_category_id]': m_c_id,
            'user_asset_act[content]': memo,
            'commit': '保存する',
        }
        result = self._session.post(
            'https://moneyforward.com/cf/create',
            data=post_data,
            headers=headers
        )
        return True
