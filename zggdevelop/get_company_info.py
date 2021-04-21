import requests
from lxml import etree
import re
import json
import ast


def get_page_data(url):
    res = requests.get(url)
    html = etree.HTML(res.content, etree.HTMLParser(encoding='utf-8'))
    res_data = etree.tostring(html, pretty_print=True, encoding='utf-8', method="html").decode('utf-8')
    # 将false替换成0
    res_data = re.sub(r'<(/)?[a-z]+(>)?(\n)?', "", res_data).replace('false', '0')
    # 将null替换成0
    res_data = res_data.replace('null', '0')
    res_data = ast.literal_eval(res_data)
    return res_data


def get_hxbd_data(url):
    '''
    获取核心必读数据，包括：核心指标，卖空明细，机构评级
    :param url:
    :return:
    '''
    res_datas = get_page_data(url)['data']

    # 核心指标
    zyzb = res_datas["zyzb"][0]
    zyzb_date = zyzb['REPORTDATE']
    # REPORTDATE 市盈率TTM 每股收益TTM 每股股息（USD) 收入总额 归母净利润 毛利率 总股本 市净率MRQ 每股净资产 周息率 收入总额同比 归母净利润同比 净利率 总市值（USD）
    zyzb_data = [(zyzb_date, zyzb["PETEM"], zyzb["EPSTTM"], zyzb["DPS"], zyzb["OPERATEREVE"],
                  zyzb["PARENTNETPROFIT"], zyzb["SPECIALINDEX1"], zyzb["ADSNUM"], zyzb['PBMRQ'],
                  zyzb['BPS'], zyzb['DPS'], zyzb['OPERATEREVEYOY'], zyzb['PARENTNETPROFITYOY'],
                  zyzb['SPECIALINDEX2'], zyzb['TOTALVALUE'])]
    # 卖空明细
    mkmx = res_datas['mkmx']
    mkmx_data = []
    for res_data in mkmx:
        # 结算日 未平仓做空 回补天数
        temp_data = (res_data['SettleDate'], res_data['ShortInterest'], res_data['Days'])
        mkmx_data.append(temp_data)

    # 机构评级
    jgpj = res_datas['jgpj']
    jgpj_data = []
    for res_data in jgpj:
        # 机构名称 最新评级 目标价(USD) 日期
        temp_data = (res_data['SCOMPANYNAME'], res_data['SRATINGNAME'], res_data['AIMPRICE'], res_data['PUBLISHDATE'])
        jgpj_data.append(temp_data)

    return zyzb_data, mkmx_data, jgpj_data


def get_gsgk_data(url):
    '''
    获取公司概况数据，包括：证券资料，公司资料，公司介绍，高管研究, 主营构成
    :param url:
    :return:
    '''
    res_datas = get_page_data(url)['data']
    # 证券资料
    zqzl = res_datas["zqzl"][0]
    # '证券代码''证券类型' '上市日期''每股面值''ISIN' '上市场所''年结日''ADS折算比'
    zqzl_data = [(zqzl['SECURITYCODE'], zqzl['SECURITYTYPE'], zqzl['LISTEDDATE'], zqzl['PARVALUE'], zqzl['ISINCODE'],
                  zqzl['TRADEMARKET'], zqzl['FISCALDATE'], zqzl['ADSZS'])]

    # 公司资料
    gszl = res_datas['gszl'][0]
    # 公司名称 中文名称 所属行业 主席 公司网址 电邮地址 电话号码 传真号码 成立日期 员工人数 注册地址 办公地址
    gszl_data = [(gszl['COMPNAME'], gszl['COMPNAMECN'], gszl['INDUSTRY'],gszl['CHAIRMAN'], gszl['WEBSITE'], gszl['EMAIL'],
                  gszl['PHONE'],gszl['FAX'],gszl['FOUNDDATE'],gszl['EMPLOYNUM'],gszl['ADDRESS'],gszl['OFFICEADDRESS'])]

    # 公司介绍
    gsjs_data = [(gszl['COMPPROFILE'])]

    # 高管研究
    ggyj = res_datas['ggyj']
    ggyj_data = []
    for res_data in ggyj:
        # 姓名 性别 学历 出生年份 任职
        temp_data = (res_data['NAMEEN'], res_data['SEX'], res_data['EDUCATION'], res_data['BIRTHDATE'], res_data['OCCUPATION'])
        ggyj_data.append(temp_data)

    # 主营构成
    # 获得主营构成的日期用作后期合成URL
    zygc_bgq = [i['REPORTDATE'] for i in res_datas['zygc_bgq']]
    zygc_data = []
    for date in zygc_bgq:
        zygc_url = url.replace('PageAjax', 'GetZYGC_ByDate') + '&date=' + date
        res_data = get_page_data(zygc_url)['data']['zygc_cp']
        # 时间 类别，占比
        for i in res_data:
            temp_data = (i['REPORTDATE'], i['PRODUCTNAME'], i['RATIO']+'%')
            zygc_data.append(temp_data)

    return zqzl_data, gsjs_data, gsjs_data, ggyj_data, zygc_data


def get_zcfzb_data(url):
    '''
    获取资产负债表数据，包括：流动资产，非流动资产，流动负债，非流动负债，资本及储备，资产负债表汇总类
    :param url:
    :return:
    '''
    res_datas = get_page_data(url)['zcfzb_qt']
    # 流动资产
    ldzc_data = []
    for res_data in res_datas:
        # 日期 现金及现金等价物 应收账款 存货 预付款项(流动) 流动资产合计
        temp_data = (res_data['bbjzr'], res_data['xjjxjdjw'], res_data['yszk'], res_data['ch'],
                     res_data['yfzk'], res_data['ldzchj'])
        ldzc_data.append(temp_data)
    # 非流动资产
    fldzc_data = []
    for res_data in res_datas:
        # 日期 物业、厂房及设备 无形资产 商誉 其他非流动资产 非流动资产其他项目 非流动资产合计
        temp_data = (res_data['bbjzr'], res_data['wycfjsb'], res_data['wxzc'], res_data['sy'], res_data['qtfldzc'],
                     res_data['fldzcqtxm'], res_data['fldzchj'])
        fldzc_data.append(temp_data)

    # 流动负债
    ldfz_data = []
    for res_data in res_datas:
        # 日期 应付账款 短期债务 流动负债合计
        temp_data = (res_data['bbjzr'], res_data['yfzk'], res_data['dqzw'], res_data['ldfzhj'])
        ldfz_data.append(temp_data)
    # 非流动负债
    fldfz_data = []
    for res_data in res_datas:
        # 日期 递延所得税负债(非流动) 长期负债 其他非流动负债 非流动负债合计
        temp_data = (res_data['bbjzr'], res_data['dysdsfzfld'], res_data['cqfz'], res_data['qtfldfz'], res_data['fldfzhj'])
        fldfz_data.append(temp_data)

    # 资本及储备
    zbjcb_data =[]
    for res_data in res_datas:
        # 日期 普通股 库存股 留存收益 股本溢价 其他综合收益 归属于母公司股东权益 股东权益合计
        temp_data = (res_data['bbjzr'], res_data['ptg'], res_data['kcg'], res_data['lcsy'], res_data['gbyj'],
                     res_data['qtzhsy'], res_data['gsymgsgdqy'])
        zbjcb_data.append(temp_data)

    # 资产负债表汇总类
    zcfzbhzl_data = []
    for res_data in res_datas:
        # 日期 总资产 总负债 负债及股东权益合计
        temp_data = (res_data['bbjzr'], res_data['zzc'], res_data['zfz'], res_data['fzjgdqyhj'])
        zcfzbhzl_data.append(temp_data)

    return ldzc_data, fldzc_data, ldfz_data, fldfz_data, zbjcb_data, zcfzbhzl_data


def get_zhsyb_data(url):
    '''
    获取综合损益表数据，包括：收入，成本，毛利，经营费用，税前溢利，股东应占溢利，每股指标，全面收益
    :param url:
    :return:
    '''
    res_datas = get_page_data(url)['lrb_qt']
    # 收入
    yysr_data = []
    for res_data in res_datas:
        # 日期 营业收入
        temp_data = (res_data['bbjzr'], res_data['yysr'])
        yysr_data.append(temp_data)

    # 成本
    yycb_data = []
    for res_data in res_datas:
        # 日期 营业成本
        temp_data = (res_data['bbjzr'], res_data['yycb'])
        yycb_data.append(temp_data)

    # 毛利
    ml_data = []
    for res_data in res_datas:
        # 日期 毛利
        temp_data = (res_data['bbjzr'], res_data['ml'])
        ml_data.append(temp_data)
    # 经营费用
    jyfy_data = []
    for res_data in res_datas:
        # 日期 营销费用 重组费用 营业费用 其他收益 营业利润
        temp_data = (res_data['bbjzr'], res_data['yxfy'], res_data['czfy'], res_data['yyfy'], res_data['qtsy'],
                     res_data['yylr'])
        jyfy_data.append(temp_data)

    # 税前溢利
    sqyl_data = []
    for res_data in res_datas:
        # 日期 利息收入 利息支出 税前利润其他项目 持续经营税前利润 所得税 持续经营净利润 净利润
        temp_data = (res_data['bbjzr'], res_data['lxsr'], res_data['lxzc'], res_data['sqlrqtxm'], res_data['cxjysqlr'],
                     res_data['sds'], res_data['cxjyjlr'], res_data['jlr'])
        sqyl_data.append(temp_data)
    # 股东应占溢利
    gdyzyl_data = []
    for res_data in res_datas:
        # 日期 归属于普通股股东净利润 归属于母公司股东净利润
        temp_data = (res_data['bbjzr'], res_data['gsyptggdjlr'], res_data['gsymgsgdjlr'])
        gdyzyl_data.append(temp_data)

    # 每股指标
    mgzb_data = []
    for res_data in res_datas:
        # 日期 归属于普通股股东净利润 归属于母公司股东净利润
        temp_data = (res_data['bbjzr'], res_data['jbmgsyptg'], res_data['tbmgsyptg'])
        mgzb_data.append(temp_data)

    # 全面收益
    qmsy_data = []
    for res_data in res_datas:
        # 日期 本公司拥有人占全面收益总额 其他全面收益其他项目 其他全面收益合计项 全面收益总额
        temp_data = (res_data['bbjzr'], res_data['bgsyyrzqmsyze'], res_data['qtqmsyqtxm'],
                     res_data['qtqmsyhjx'], res_data['qmsyze'])
        qmsy_data.append(temp_data)

    return yysr_data, yycb_data, ml_data, jyfy_data, sqyl_data, gdyzyl_data, mgzb_data, qmsy_data


def get_xjllb_data(url):
    '''
    获取现金流量表数据，包括：经营业务调整，经营业务，投资业务，筹资业务， 现金流量表汇总类
    :param url:
    :return:
    '''
    res_datas = get_page_data(url)['xjllb']
    # 经营业务调整
    jyywtz_data = []
    for res_data in res_datas:
        # 日期 净利润 折旧及摊销 基于股票的补偿费 减值及拨备 递延所得税 资产处置损益 养老及退休福利 经营业务调整其他项目
        temp_data = (res_data['bbjzr'], res_data['jlr'], res_data['zjjtx'], res_data['jygpdbck'], res_data['jzjbb'],
                     res_data['dysds'], res_data['zcczsy'], res_data['yljtxfl'], res_data['jyywtzqtxm'])
        jyywtz_data.append(temp_data)
    # 经营业务
    jyyw_data = []
    for res_data in res_datas:
        # 日期 应收账款及票据 存货 待摊费用及其他资产 应付账款及票据 应付税项 经营业务其他项目 经营活动产生的现金流量净额
        temp_data = (res_data['bbjzr'], res_data['yszkjpj'], res_data['yfzkjpj'], res_data['ch'], res_data['dtfyjqtzc'],
                     res_data['yfsx'], res_data['jyywjqtxm'], res_data['jyhdcsdxjllje'])
        jyyw_data.append(temp_data)
    # 投资业务
    tzyw_data = []
    for res_data in res_datas:
        # 日期 购买固定资产 处置固定资产 购建无形资产及其他资产 其他投资活动产生的现金流量净额 投资活动产生的现金流量净额
        temp_data = (res_data['bbjzr'], res_data['gmgdzc'], res_data['czgdzc'], res_data['gjwxzcjqtzc'],
                     res_data['qttzhdcsdxjllje'], res_data['tzhdcsdxjllje'])
        tzyw_data.append(temp_data)
    # 筹资业务
    czyw_data = []
    for res_data in res_datas:
        # 日期 发行股份 回购股份 发行债券 赎回债券 股息支付 偿还借款 发行费用相关 筹资业务其他项目 筹资活动产生的现金流量净额
        temp_data = (res_data['bbjzr'], res_data['fxgf'], res_data['hggf'], res_data['fxzq'], res_data['shzq'],
                     res_data['gxzf'], res_data['chjk'], res_data['fxfyxg'], res_data['czywqtxm'], res_data['czhdcsdxjllje'])
        czyw_data.append(temp_data)
    # 现金流量表汇总类
    xjllbhzl_data = []
    for res_data in res_datas:
        # 日期 现金及现金等价物增加(减少)额 现金及现金等价物期初余额 现金及现金等价物期末余额
        temp_data = (res_data['bbjzr'], res_data['xjjxjdjwzjjse'], res_data['xjjxjdjwqcye'], res_data['xjjxjdjwqmye'])
        xjllbhzl_data.append(temp_data)

    return jyywtz_data, jyyw_data, tzyw_data, czyw_data, xjllbhzl_data


def get_fhpx_data(url):
    '''
    获取分红派息数据，包括：公告日期 分红方式 方案说明 截止过户日 现金发放日 除权除息日
    :param url:
    :return:
    '''
    res_datas = get_page_data(url)['fhpx']
    # 如果没有数据返回None
    if len(res_datas) == 0:
        return None
    fhpx_data = []
    for res_data in res_datas:
        # 公告日期 分红方式 方案说明 截止过户日 现金发放日 除权除息日
        temp_data = (res_data['ggrq'], res_data['fhfs'], res_data['fasm'], res_data['jzghr'],
                     res_data['xjffr'], res_data['cqcxr'])
        fhpx_data.append(temp_data)
    return fhpx_data


def get_gbgd_data(url):
    '''
    获取股本股东数据，包括:拆股并股，股本变动
    :param url:
    :return:
    '''
    res_datas = get_page_data(url)['data']
    # 拆股并股
    cgbg_data = []
    # 如果没有数据，为None
    if len(res_datas['cgbg']) == 0:
        cgbg_data = None
    else:
        for res_data in res_datas['cgbg']:
            # 公告日期 方案说明 除权日
            temp_data = (res_data['UPDATEDATE'], res_data['EDESCRIBE'], res_data['EXDATE'])
            cgbg_data.append(temp_data)

    # 股本变动数据
    gbbd_data = []
    for res_data in res_datas['gbbd']:
        # 变动日期 已发行普通股 已发行优先股
        temp_data = (res_data['CHANGEDATE'], res_data['NORMALISSUEVOLUMN'], res_data['PREFERREDISSUEVOLUMN'])
        gbbd_data.append(temp_data)

    return cgbg_data, gbbd_data


def get_jgcg_data(url):
    '''
    获取机构持股数据，包括：发布日期 机构家数 持股总数 持股比例 新进机构 增持机构 减持机构
    :param url:
    :return:
    '''
    res_datas = get_page_data(url)['data']
    jgcg_data = []
    for res_data in res_datas['jgcg_hz']:
        # 发布日期 机构家数 持股总数 持股比例 新进机构 增持机构 减持机构
        temp_data = (res_data['REPORTDATE'], res_data['ORGNUM'], res_data['SHAREHDNUMSUM'], res_data['SHAREHDRATIOJSSUM'],
                     res_data['NEWORGNUM'], res_data['LONGORGNUM'], res_data['SHORTORGNUM'])
        jgcg_data.append(temp_data)

    return jgcg_data


def get_jjcg_data(url):
    '''
    获取基金持股数据，包括：发布日期 基金名称 持有股数（股）持股比例	变动股数（股）	变动比例
    :param url:
    :return:
    '''
    res_datas = get_page_data(url)['data']
    jjcg_data = []
    for res_data in res_datas['jjcg']:
        # 发布日期 基金名称 持有股数（股）持股比例	变动股数（股）	变动比例
        temp_data = (res_data['REPORTDATE'], res_data['FUNDNAME'], res_data['SHAREHDNUM'], res_data['SHAREHDRATIO'],
                     res_data['CHGNUM'], res_data['CHGRATIO'])
        jjcg_data.append(temp_data)

    return jjcg_data


def get_ggcg_data(url):
    '''
    获取高管持股数据，包括：交易日期 高管名称 变动后持股（股） 最新变动 交易股份（股） 交易价格（USD） 职务	持有类型
    :param url:
    :return:
    '''
    res_datas = get_page_data(url)['data']
    ggcg_data = []
    if len(res_datas['ggcg']) == 0:
        ggcg_data = None
    else:
        for res_data in res_datas['ggcg']:
            # 交易日期 高管名称 变动后持股（股） 最新变动 交易股份（股） 交易价格（USD） 职务	持有类型
            temp_data = (res_data['TRADEDATE'], res_data['PERSONNAME'], res_data['SHARESHOLD'], res_data['TRADETYPE'],
                         res_data['TRADESHNUM'], res_data['PRICE'], res_data['RELATION'], res_data['OWNERTYPE'], )
            ggcg_data.append(temp_data)

    return ggcg_data


def main(symbol, postfix):
    '''
    获取所有公司数据
    :param symbol: 股票代码
    :param postfix: 后缀
    :return:
    '''
    # 核心必读数据
    # 核心指标，卖空明细，机构评级
    hxbd_url = f'http://emweb.eastmoney.com/pc_usf10/CoreReading/PageAjax?fullCode={symbol}.{postfix}'
    zyzb_data, mkmx_data, jgpj_data = get_hxbd_data(hxbd_url)
    print('核心必读：', zyzb_data, mkmx_data, jgpj_data)
    # 公司概况数据
    # 证券资料，公司资料，公司介绍，高管研究, 主营构成
    gsgk_url = f'http://emweb.eastmoney.com/pc_usf10/CompanyInfo/PageAjax?fullCode={symbol}.{postfix}'
    zqzl_data, gsjs_data, gsjs_data, ggyj_data, zygc_data = get_gsgk_data(gsgk_url)
    print('公司概况', zqzl_data, gsjs_data, gsjs_data, ggyj_data, zygc_data)
    # 资产负债表数据
    # 流动资产，非流动资产，流动负债，非流动负债，资本及储备，资产负债表汇总类
    zcfzb_url = f"http://emweb.eastmoney.com/pc_usf10/FinancialAnalysis/GetZCFZB?SecurityCode={symbol}.{postfix}&ReportDateType=2&StatisType=1&CompanyType=4&StartIndex=0"
    ldzc_data, fldzc_data, ldfz_data, fldfz_data, zbjcb_data, zcfzbhzl_data = get_zcfzb_data(zcfzb_url)
    print('资产负债表', ldzc_data, fldzc_data, ldfz_data, fldfz_data, zbjcb_data, zcfzbhzl_data)
    # 综合损益表数据
    # 收入，成本，毛利，经营费用，税前溢利，股东应占溢利，每股指标，全面收益
    zhsyb_url = f'http://emweb.eastmoney.com/pc_usf10/FinancialAnalysis/GetLRB?SecurityCode={symbol}.{postfix}&ReportDateType=2&StatisType=1&CompanyType=4&StartIndex=0'
    yysr_data, yycb_data, ml_data, jyfy_data, sqyl_data, gdyzyl_data, mgzb_data, qmsy_data = get_zhsyb_data(zhsyb_url)
    print('综合损益表', yysr_data, yycb_data, ml_data, jyfy_data, sqyl_data, gdyzyl_data, mgzb_data, qmsy_data)
    # 现金流量表
    # 经营业务调整，经营业务，投资业务，筹资业务， 现金流量表汇总类
    xjllb_url = f'http://emweb.eastmoney.com/pc_usf10/FinancialAnalysis/GetXJLLB?SecurityCode={symbol}.{postfix}&ReportDateType=2&StatisType=1&CompanyType=4&StartIndex=0'
    jyywtz_data, jyyw_data, tzyw_data, czyw_data, xjllbhzl_data = get_xjllb_data(xjllb_url)
    print('现金流量表', jyywtz_data, jyyw_data, tzyw_data, czyw_data, xjllbhzl_data)
    # 分红派息
    # 公告日期 分红方式 方案说明 截止过户日 现金发放日 除权除息日
    fhpx_url = f'http://emweb.eastmoney.com/pc_usf10/ShareBonus/GetShareBonusData?SecurityCode={symbol}.{postfix}&PageNum=1&PageSize=25'
    fhpx_data = get_fhpx_data(fhpx_url)
    print('分红派息', fhpx_data)
    # 股本股东
    # 拆股并股，股本变动
    gbgd_url = f'http://emweb.eastmoney.com/pc_usf10/Shareholders/PageAjax?fullCode={symbol}.{postfix}'
    cgbg_data, gbbd_data = get_gbgd_data(gbgd_url)
    print('股本股东', cgbg_data, gbbd_data)
    # 机构持股
    jgcg_url = f'http://emweb.eastmoney.com/pc_usf10/Shareholders/JGCGAjax?fullCode={symbol}.{postfix}'
    jgcg_data = get_jgcg_data(jgcg_url)
    print('机构持股', jgcg_data)
    # 基金持股
    jjcg_url = f'http://emweb.eastmoney.com/pc_usf10/Shareholders/JJCGAjax?fullCode={symbol}.{postfix}'
    jjcg_data = get_jjcg_data(jjcg_url)
    print('基金持股', jjcg_data)
    # 高管持股
    ggcg_url = f'http://emweb.eastmoney.com/pc_usf10/Shareholders/GGCGAjax?fullCode={symbol}.{postfix}'
    ggcg_data = get_ggcg_data(ggcg_url)
    print('高管持股', ggcg_data)


main('BABA', 'N')