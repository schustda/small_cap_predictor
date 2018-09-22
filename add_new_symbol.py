from etl.ihub_data import IhubData
from etl.stock_data import StockData
from model.combine_data import CombineData
from model.training_data import TrainingData
from src.general_functions import GeneralFunctions
from time import sleep
import pandas as pd
from sys import argv


def add_new_symbol(symbol,ihub_code):
    '''
    To reduce errors, any given stock will be added to the database one at a
        time. The user can specify which stock is added. The only input needed
        is the symbol, and the ihub link to that stock's message board
    '''
    df = pd.DataFrame(columns=['symbol','name','ihub_code'])


    # symbol = input('What is the symbol? ').lower()
    # name = input('What is the company name? ')
    # ihub_code = input('What is the iHub Message Board URL?  ')

    name = ' '.join(ihub_code.split('-')[:-2])
    df.loc[0] = [symbol,name,ihub_code]
    gf = GeneralFunctions()
    gf.to_table(df,'items.symbol')
    sleep(2)

    symbol_id = gf.get_value('symbol_id',replacements={'{symbol}':symbol})
    print (symbol_id)

    ihub = IhubData(verbose=1,delay=True)
    ihub.update_posts(symbol_id)

    sd = StockData()
    sd.update_stock_data(symbol_id)

    cd = CombineData()
    cd.compile_data(symbol_id)

    td = TrainingData(verbose=1)
    td.working_split(symbol_id)


if __name__ == '__main__':

    add_new_symbol(argv[1],argv[2])
    # add_new_symbol('rbiz','RealBiz-Media-Group-Inc-RBIZ-25681')
    #
    # new_stocks = [
    #     ['libe','Liberated-Solutions-Inc-LIBE-26214'],
    #     ['aagc','All-American-Gold-Corp-AAGC-20442'],
    #     ['amfe','Amfil-Technologies-Inc-AMFE-12460'],
    #     ['adtm','Adaptive-Medias-Inc-ADTM-18824'],
    #     ['amlh','American-Leisure-Holdings-Inc-AMLH-29447'],
    #     ['aphd','Appiphany-Technologies-Holdings-Corp-APHD-25054'],
    #     ['avxl','Anavex-Life-Sciences-Corp-AVXL-11474'],
    #     ['azfl','Amazonas-Florestal-Ltd-AZFL-25536'],
    #     ['brvo','Bravo-Multinational-Inc-BRVO-16794'],
    #     ['bvtk','Bravatek-Solutions-Inc-BVTK-17852'],
    #     ['byoc','Beyond-Commerce-INC-BYOC-15218'],
    #     ['cbyi','Cal-Bay-International-Inc-CBYI-5520'],
    #     ['cnxs','Connexus-Corp-CNXS-17863'],
    #     ['coho','Crednology-Holding-Corp-COHO-4899'],
    #     ['cytr','Cytrx-Corp-CYTR-3392'],
    #     ['dcth','Delcath-Systems-Inc-DCTH-3897'],
    #     ['jbzy','JB&ZJMY-Holding-Company-JBZYD-16401'],
    #     ['ecos','EcoloCap-Solutions-Inc-ECOS-347'],
    #     ['eled','ExeLED-Holdings-Inc-ELED-27161'],
    #     ['snpw','Sun-Pacific-Holding-Corp-SNPW-11015'],
    #     ['fern','Fernhill-Corporation-FERN-13682'],
    #     ['hmpq','HempAmericana-Inc-HMPQ-29604'],
    #     ['icnb','Iconic-Brands-Inc-ICNB-15837'],
    #     ['ihsi','Intelligent-Highway-Solutions-Inc-IHSI-26561'],
    #     ['ipix','Innovation-Pharmaceuticals-Inc-IPIX-12580'],
    #     ['kget','CaliPharms-Inc-KGET-10313'],
    #     ['lqmt','Liquidmetal-Technologies-LQMT-3856'],
    #     ['mine','Minerco-Inc-MINE-17939'],
    #     ['npwz','Neah-Power-Systems-Inc-NPWZ-7635'],
    #     ['nwbo','NorthWest-Biotherapeutics-Inc-NWBO-3441'],
    #     ['ocsy','Optium-Cyber-Systems-Inc-OCSY-10536'],
    #     ['omvs','On-the-Move-Systems-Corp-OMVS-21568'],
    #     ['onci','On4-Communications-Inc-ONCI-6203'],
    #     ['ottv','Viva-Entertainment-Group-Inc-OTTV-25564'],
    #     ['owcp','OWC-Pharmaceutical-Research-Corp-OWCP-16859'],
    #     ['pgpm','Pilgrim-Petroleum-Corp-PGPM-5655'],
    #     ['rdar','Raadr-Inc-RDAR-14382'],
    #     ['rmrk','Rimrock-Gold-Corp-RMRK-17049'],
    #     ['rtnb','Root9B-Holdings-Inc-RTNB-8167'],
    #     ['sanp','Santo-Mining-Corp-SANP-24962'],
    #     ['sfrx','Seafarer-Exploration-Corp-SFRX-5020'],
    #     ['sigo','Sunset-Island-Group-Inc-SIGO-5724'],
    #     ['stbv','Strategic-Global-Investments-Inc-STBV-14702'],
    #     ['suti','SUTIMCo-International-Inc-SUTI-17155'],
    #     ['uoip','UnifiedOnline-Inc-UOIP-5196'],
    #     ['vdrm','ViaDerma-Inc-VDRM-14324'],
    #     ['vpor','Vapor-Group-Inc-VPOR-18541'],
    #     ['wmih','WMIH-Corp-WMIH-11133'],
    #     ['xtrn','Las-Vegas-Railway-Express-XTRN-16650'],
    #     ['cwir','Central-Wireless-Inc-CWIR-1695'],
    #     ['frfs','Firefish-Inc-FRFS-24668'],
    #     ['atpt','All-State-Properties-Holdings-Inc-ATPT-7400'],
    #     ['mves','The-Movie-Studio-Inc-MVES-8601'],
    #     ['tggi','Trans-Global-Group-Inc-TGGI-9022'],
    #     ['tbev','High-Performance-Beverages-Comp-TBEV-25157'],
    #     ['digaf','Digatrade-Financial-Corp-DIGAF-20186'],
    #     ['wwio','Wowio-Inc-WWIO-28310'],
    #     ['nphc','Nutra-Pharma-Corp-NPHC-5678'],
    #     ['vgid','V-Group-Inc-VGID-20860'],
    #     ['frlf','Freedom-Leaf-Inc-FRLF-30128'],
    #     ['bstn','Boston-Carriers-Inc-BSTN-9323'],
    #     ['asck','Auscrete-Corp-ASCK-30472'],
    #     ['pdxp','PDX-Partners-Inc-PDXP-11089'],
    #     ['myec','MyECheck-Inc-MYEC-11137'],
    #     ['aryc','Arrayit-Corporation-ARYC-9031'],
    #     ['cbis','Cannabis-Science-Inc-CBIS-7105'],
    #     ['ecmh','Encompass-Holdings-(Aqua-Xtremes)-ECMH-5508'],
    #     ['bvtk','Bravatek-Solutions-Inc-BVTK-17852'],
    #     ['ptop','Peer-To-Peer-Network-PTOP-24870'],
    #     ['kosk','One-Step-Vending-Corp-KOSK-7247'],
    #     ['nsav','NSAV-Holding-Inc-NSAV-16357'],
    #     ['svte','Service-Team-SVTE-30204'],
    #     ['shmn','SOHM-INC-SHMN-15560'],
    #     ['vplm','Voip-PalCom-Inc-VPLM-18089'],
    #     ['gohe','Global-Payout-Inc-GOHE-13263'],
    #     ['cctl','Coin-Citadel-CCTL-14957'],
    #     ['amhd','HLK-Biotech-Holding-Group-Inc-AMHD-3967'],
    #     ['vida','Vidaroo-Corporation-VIDA-14139'],
    #     ['phot','GrowLife-Inc-PHOT-12210'],
    #     ['hpnn','Hop-On-Inc-HPNN-1900'],
    #     ['anfc','Black-Ridge-Oil-and-Gas-Inc-ANFC-21955'],
    #     ['stsc','Start-Scientific-Inc-STSC-25441'],
    #     ['emhtf','Emerald-Health-Therapeutics-Inc-EMHTF-32237'],
    #     ['acbff','Aurora-Cannabis-Inc-ACBFF-30411'],
    #     ['bayp','Bayport-International-Holdings-Inc-BAYP-10612'],
    #     ['intv','Integrated-Ventures-Inc-INTV-29895'],
    #     ['mspc','Metrospaces-Inc-MSPC-10340'],
    #     ['ldsr','Landstar-Inc-LDSR-1183'],
    #     ['ssof','Sixty-Six-Oilfield-Services-Inc-SSOF-26960']
    #     ]
    #
    #
    # for num, x in enumerate(new_stocks):
    #     add_new_symbol(x[0], x[1])

    # odd
    # for num, x in enumerate(new_stocks):
    #     if not num % 2:
    #         print ('NEXT')
    #         print (x[0], x[1])
    #         add_new_symbol(x[0], x[1])

    # even
    # for num, x in enumerate(new_stocks):
    #     if num % 2:
    #         print ('NEXT')
    #         print (x[0], x[1])
    #         add_new_symbol(x[0], x[1])
