# -*- coding: utf-8 -*-
import asyncio
import re

import aiohttp
import aiomysql
from lxml import etree

from conn_mysql import get_data_by_column_sync

pool = None
# sem = asyncio.Semaphore(4)  用来控制并发数，不指定会全速运行
stop = False
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.71 '
                  'Safari/537.36 Core/1.94.244.400 QQBrowser/12.5.5646.400',
    'Cookie': 'zhishiTopicRequestTime=1716633545697; BAIDUID=B9E10E9A721548BB969246E39B5F47E5:FG=1; PSTM=1712820509; '
              'BIDUPSID=1EE99BBE91590BC0A35424CA05868494; '
              'BDUSS'
              '=dmdDM3NllsbWxmeXR0Z0JFemxWNDAyelJPbVlmcE1UT2Z2RWNzalZocVBOejltSVFBQUFBJCQAAAAAAAAAAAEAAABxu19jc3PV5rXEvNm1xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAI-qF2aPqhdmVH; BDUSS_BFESS=dmdDM3NllsbWxmeXR0Z0JFemxWNDAyelJPbVlmcE1UT2Z2RWNzalZocVBOejltSVFBQUFBJCQAAAAAAAAAAAEAAABxu19jc3PV5rXEvNm1xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAI-qF2aPqhdmVH; H_PS_PSSID=60270_60277_60283_60288_60299_60253; H_WISE_SIDS=60270_60277_60283_60288_60299_60253; H_WISE_SIDS_BFESS=60270_60277_60283_60288_60299_60253; BAIDUID_BFESS=B9E10E9A721548BB969246E39B5F47E5:FG=1; BA_HECTOR=a0000h8h0580812125218l0ld5ng551j52r6g1v; ZFY=7zYNGmj1ib9E4elWCOldyid:AveZZJ7OhlI:BjqvHg47s:C; BDORZ=FFFB88E999055A3F8A630C64834BD6D0; channel=baidusearch; __bid_n=18faf52a1090576e697fff; BDRCVFR[S_ukKV6dOkf]=mk3SLVN4HKm; delPer=0; PSINO=7; RT="z=1&dm=baidu.com&si=a5ac59e5-64c6-4a4b-9295-f789e09609ff&ss=lwlqpdei&sl=5u&tt=2bb2&bcn=https%3A%2F%2Ffclog.baidu.com%2Flog%2Fweirwood%3Ftype%3Dperf&ld=9fwr2"; baikeVisitId=87622590-d9b4-4b29-ad4b-d5a594b54fa3; ab_sr=1.0.1_MDdlOWM4MjFhZmMwZTBiY2RjNDkwMjhkZjRkMjRjZDY3MWJmOTcxYTI1ZTI1MGY0NGVhN2I5NjA4YzkzM2Q2ZTIxYzM0NjVjMzk3YWYyYzhkYjlhYjEzMTVhYjU4OTQ5MzIwZDU0YjAwNDlkMWNlODk0ZWZhM2M3NzUzYjBkNzc0NGU3ZmM1OWMwZjM5YmI1YTAyZTcyMWQxNzlhZGJlZDQ3ODRkYjM3MzU0MGQyMTNmZTI2OGE2NTE0MGY3ZWJi'
}
TABLE_NAME = 'starstable'  # 保存到那张表
url = 'https://baike.baidu.com/item/{}'  # url地址拼接
# 获取那一列的数据
user_name = get_data_by_column_sync('star', True)
urls = []  # 所有页的url列表


async def fetch(url, session):
    '''
    aiohttp获取网页源码
    '''
    try:
        async with session.get(url, headers=headers, verify_ssl=False) as resp:
            if resp.status in [200, 201]:
                return await resp.text()
    except Exception as e:
        print(e)
        return None


def clean_value(value):
    # 清理值中的杂质，如'[95]'
    return re.sub(r'\[\d+\]', '', value)


def set_default_if_not_found(desired_field, key_data, value_data):
    """如果desired_field在key_data中不存在，则返回'null'，否则返回对应的value_data值"""
    for index, field in enumerate(key_data):
        if field.replace('\xa0', ' ').strip().lower() == desired_field.replace('\xa0', ' ').strip().lower():
            value = value_data[index]
            return clean_value(value.strip()) if isinstance(value, str) else 'null'
    return 'null'


def extract_elements(source, userName):
    '''
    提取出详情页里面的详情内容
    '''
    try:
        dom = etree.HTML(source)

        # 提取基本信息
        key_data = dom.xpath('//div[@class="basicInfo_spa7J J-basic-info"]//dt/text()')
        value_data = [clean_value(' '.join(data.xpath('.//text()')).strip()) for data in
                      dom.xpath('//div[@class="basicInfo_spa7J J-basic-info"]//dd')]

        # 提取简介
        temp_list = dom.xpath("//div[@class='lemmaSummary_cFhDf J-summary']//text()")
        cleaned_string = re.sub(r'\[[^\]]*\]', '', ''.join(temp_list).strip())

        # 定义的字段列表（注意移除了“本名”，因为它将直接设置）
        desired_fields = [
            '性\xa0\xa0\xa0\xa0别', '出生日期', '出生地', '身\xa0\xa0\xa0\xa0高', '代表作品',
            '主要成就'
        ]

        # 初始化结果字典并设置“本名”字段
        information = {'本名': userName}

        # 使用字典推导式填充除“本名”之外的其他字段
        information.update({
            desired_field.replace('\xa0', '').strip(): set_default_if_not_found(desired_field, key_data, value_data)
            for desired_field in desired_fields
        })

        information['简介'] = cleaned_string

        # 打印结果（可选，根据需求决定是否打印）
        print(information)
        return information
    except Exception as e:
        print(f'解析详情页出错！错误信息：{e}')
        return {}


async def save_to_database(information, pool):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"INSERT INTO {TABLE_NAME} (name, sex, birthplace, birthday, height, "
                "representativeWorks, mainAchievements, introduction) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    information['本名'],
                    information['性别'],
                    information['出生地'],
                    information['出生日期'],
                    information['身高'],
                    information['代表作品'],
                    information['主要成就'],
                    information['简介']
                )
            )
            await conn.commit()

    print('插入数据成功')


async def process_director(session, url, pool, userName):
    source = await fetch(url, session)
    if source:
        information = extract_elements(source, userName)
        if information:
            await save_to_database(information, pool)


async def main():
    global pool
    pool = await aiomysql.create_pool(host='127.0.0.1', port=3306,
                                      user='root', password='123456',
                                      db='fileknowledge', loop=None, charset='utf8',
                                      autocommit=True)

    async with aiohttp.ClientSession() as session:
        tasks = []
        for userName in user_name:
            formatted_url = url.format(userName)
            task = asyncio.create_task(process_director(session, formatted_url, pool, userName))
            tasks.append(task)

        await asyncio.gather(*tasks)

        pool.close()
        await pool.wait_closed()

    print('所有任务完成，连接池已关闭')


if __name__ == '__main__':
    asyncio.run(main())
