# -*- coding: utf-8 -*
import urllib2
import xml.etree.ElementTree as ET
from lxml import etree
import sqlite3
import time
from dateutil import rrule
import datetime


# копирует список уведомлений в файл export.xml за период date1-date2


def dllist(date1, date2):
    url = "https://torgi.gov.ru/opendata/7710349494-torgi/data.xml?bidKind=2&publishDateFrom=%s&publishDateTo=%s," % (
        date1, date2)
    while True:
        try:
            s = urllib2.urlopen(url)
        except urllib2.URLError:
            print('Ошибка!')
            time.sleep(20)
            pass
        else:
            break
    contents = s.read()
    file = open("export.xml", 'w')
    file.write(contents)
    file.close()


# экспорт строки данных в бд
def insertsql(row):
    conn = sqlite3.connect('auct.db')
    conn.text_factory = str
    c = conn.cursor()
    c.execute('INSERT INTO data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', row)
    conn.commit()


# парсим загруженный файл и выводим основные параметры участка

def mainloop():
    tree = ET.parse('export2.xml')
    root = tree.getroot()

    # используется lxml для извлечения пространства имен'{http://torgi.gov.ru/opendata}'
    tree2 = etree.parse('export2.xml')
    xmlns = '{%s}' % tree2.xpath('namespace-uri(.)')
    for lot in root.iterfind('./%snotification/' % xmlns):
        checkid = 0
        if lot.tag == '%slot' % xmlns:
            limit = 0
            lotinfo = []
            # проверка по bidType - состоявшийся и завершенный с одним участником
            '''
            if (lot.find('./%sbidStatus/%sid' % (xmlns, xmlns)).text.encode('utf-8') == '5' 
                or lot.find('./%sbidStatus/%sid' % (xmlns, xmlns)).text.encode('utf-8') == '6'):
                print 'Состоялся!'
            else:
                checkid = checkid + 1
            '''

            # проверка на принадлежность категории участков !добавить растениеводство в mission
            def checkcx(path):
                if hasattr(path, 'text'):
                    if 'ельскохоз' in path.text.encode('utf-8') or 'водство' in path.text.encode('utf-8'):
                        return True
                    else:
                        return False
                else:
                    return False

            # проверка на наличие текстовых атрибутов и добавляем их в список
            def check(path2):
                if hasattr(path2, 'text'):
                    try:
                        path2.text.encode('utf-8')
                    except:
                        lotinfo.append('н/д')
                    else:
                        lotinfo.append(path2.text.encode('utf-8'))
                else:
                    lotinfo.append('н/д')

            # перечень данных на импорт
            if (limit == 0 and checkid == 0 and (checkcx(lot.find('./%sgroundType/%sname' % (xmlns, xmlns)))
                                                 or checkcx(
                        lot.find('./%sgroundUsage/%sname' % (xmlns, xmlns))) or checkcx(
                        lot.find('./%smission' % xmlns)))):
                print('yes')
                limit = limit + 1
                # добавляем ссылку к лотам
                lotinfo.append(url2)
                # id лота
                check(lot.find('%sid' % xmlns))
                # дата сделки
                check(root.find('./%snotification/%scommon/%spublished' % (xmlns, xmlns, xmlns)))
                check(root.find('./%snotification/%scommon/%sbidAuctionDate' % (xmlns, xmlns, xmlns)))

                check(lot.find('./%skladrLocation/%sid' % (xmlns, xmlns)))
                check(lot.find('./%skladrLocation/%sname' % (xmlns, xmlns)))
                check(lot.find('%slocation' % xmlns))

                check(lot.find('%sarea' % xmlns))
                check(lot.find('./%sunit/%sname' % (xmlns, xmlns)))

                check(lot.find('%scadastralNum' % xmlns))

                check(lot.find('./%sgroundType/%sname' % (xmlns, xmlns)))
                check(lot.find('./%sgroundUsage/%sname' % (xmlns, xmlns)))
                check(lot.find('./%smission' % xmlns))
                # начальная цена
                check(lot.find('./%sarticle/%sname' % (xmlns, xmlns)))
                check(lot.find('%stermYear' % xmlns))
                check(lot.find('%sstartPrice' % xmlns))
                check(lot.find('%sarticleVal' % xmlns))

                check(lot.find('./%sbidStatus/%sname' % (xmlns, xmlns)))
                check(lot.find('./%sbidType/%sname' % (xmlns, xmlns)))
                check(lot.find('./%spropKind/%sname' % (xmlns, xmlns)))
                insertsql(lotinfo)
            else:
                print('no!')


# открывает файл export.xml и открывает ссылку <odDetailedHref>
# для каждого элемента <notification> в дочернем элементе <odDetailedHref> поочереди сохраняем в файл export2xml
def dllots():
    tree1 = ET.parse('export.xml')
    root1 = tree1.getroot()

    tree3 = etree.parse('export.xml')
    xmlns2 = '{%s}' % tree3.xpath('namespace-uri(.)')
    for child in root1:
        xml_error = 0
        if hasattr(child.find('%sodDetailedHref' % xmlns2), 'text'):
            while True:
                try:
                    # сохраняем отдельно файл с подробностями лотов!!! поправить на нормальную ссылку
                    if xml_error == 3:
                        break
                    url2 = child.find('%sodDetailedHref' % xmlns2).text
                    global url2
                    # попробуем защититься от ошибки
                    while True:
                        try:
                            s2 = urllib2.urlopen(url2)
                        except urllib2.URLError:
                            print('Ошибка!')
                            time.sleep(20)
                            pass
                        else:
                            break
                    contents2 = s2.read()
                    file2 = open("export2.xml", 'w')
                    file2.write(contents2)
                    file2.close()
                    temp_parse = ET.parse('export2.xml')
                except:
                    print('Ошибка содержания xml!')
                    time.sleep(20)
                    xml_error += 1
                    pass
                else:
                    mainloop()
                    break


start = datetime.date(2017, 4, 1)
start2 = datetime.date(2017, 5, 1)
end = datetime.date(2019, 1, 1)
end2 = datetime.date(2019, 2, 1)


# добавляем ноль в формат месяца
def month(m):
    if m < 10:
        return '0%s' % m
    else:
        return m


# обрабатываем все за установленный период
def gogo():
    for dt, dt2 in zip(rrule.rrule(rrule.MONTHLY, dtstart=start, until=end),
                       rrule.rrule(rrule.MONTHLY, dtstart=start2, until=end2)):
        a = '%s%s01T0000' % (dt.timetuple()[0], month(dt.timetuple()[1]))
        b = '%s%s01T0000' % (dt2.timetuple()[0], month(dt2.timetuple()[1]))
        dllist(a, b)
        dllots()


gogo()
