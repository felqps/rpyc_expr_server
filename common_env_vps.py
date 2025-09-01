#!/cygdrive/c/Anaconda3/python.exe

import sys
import os

def srcs_vps(pysrc):
    qpscloud_relver = "lnx502"
    pysrc['VpsOmsAgent'] = "/Fairedge_dev/app_Vps/VpsOmsAgent.py:lnx501"
    pysrc['PganFileReceiver'] = "/Fairedge_dev/app_Vps/PganFileReceiver.py:lnx501"
    pysrc['PganFileSender'] = "/Fairedge_dev/app_Vps/PganFileSender.py:lnx501"
    pysrc['PganMsgReceiver'] = "/Fairedge_dev/app_Vps/PganMsgReceiver.py:lnx501"
    pysrc['PganMsgSender'] = "/Fairedge_dev/app_Vps/PganMsgSender.py:lnx501"
    pysrc['PganSavePnlToInfluxdb'] = "/Fairedge_dev/app_Vps/PganSavePnlToInfluxdb.py:lnx501"
    pysrc['VpsPosInfoToDb'] = "/Fairedge_dev/app_Vps/VpsPosInfoToDb.py:lnx501"
    pysrc['VpsQuoteSend'] = "/Fairedge_dev/app_Vps/VpsQuoteSend.py:lnx501"
    pysrc['adjust_tgts'] = "/Fairedge_dev/app_portsimu/adjust_tgts.py:lnx501"
    pysrc['send_target_fac'] = "/Fairedge_dev/app_portsimu/send_target_fac.py:lnx501"
    pysrc['gen_fac_fils'] = "/Fairedge_dev/app_portsimu/gen_fac_fils.py:lnx501"
    pysrc['PortTgtsSend'] = "/Fairedge_dev/app_portsimu/PortTgtsSend.py:lnx501"
    pysrc['VpsSaveVpsBar'] = "/Fairedge_dev/app_Vps/VpsSaveVpsBar.py:lnx501"
    pysrc['VpsBarApply'] = "/Fairedge_dev/app_Vps/VpsBarApply.py:lnx501"
    pysrc['ZxinBarToQuote'] = "/Fairedge_dev/app_Vps/ZxinBarToQuote.py:lnx501"
    pysrc['VpsFeedForwarder'] = "/Fairedge_dev/app_Vps/VpsFeedForwarder.py:lnx501"
    pysrc['VpsExchSimu'] = "/Fairedge_dev/app_Vps/VpsExchSimu.py:lnx501"
    pysrc['VpsFeedSimu'] = "/Fairedge_dev/app_Vps/VpsFeedSimu.py:lnx501"
    pysrc['genClo'] = "/Fairedge_dev/app_QpsLink/genClo.py:lnx501"
    pysrc['genOpn'] = "/Fairedge_dev/app_QpsLink/genOpn.py:lnx501"

    # pysrc['FCWorker'] = f"/Fairedge_dev/app_qpscloud/FCWorker.py:{qpscloud_relver}"
    pysrc['FCWorker'] = f"/Fairedge_dev/app_qpscloud/FCWorker.py:lnx506"
    pysrc['FCClient'] = f"/Fairedge_dev/app_qpscloud/FCClient.py:{qpscloud_relver}"
    pysrc['FCAdvancedClient'] = f"/Fairedge_dev/app_qpscloud/FCAdvancedClient.py:{qpscloud_relver}"
    pysrc['FCScheduler'] = f"/Fairedge_dev/app_qpscloud/FCScheduler.py:lnx501"
    pysrc['FCMessageRecord'] = f"/Fairedge_dev/app_qpscloud/FCMessageRecord.py:lnx506"
    pysrc['StatusFile'] = f"/Fairedge_dev/app_qpscloud/StatusFile.py:{qpscloud_relver}"
    pysrc['mdl_driver_QpsModelStkLOClose_A'] = f'/Fairedge_dev/app_tgen/mdl_driver_QpsModelStkLOClose_A.py:{qpscloud_relver}'
    pysrc['mdl_driver_QpsModelStkLOClose'] = f'/Fairedge_dev/app_tgen/mdl_driver_QpsModelStkLOClose.py:{qpscloud_relver}'
    pysrc['mdl_driver_QpsModelStkLO'] = f'/Fairedge_dev/app_tgen/mdl_driver_QpsModelStkLO.py:{qpscloud_relver}'
    pysrc['VpsBarMaker'] = f'/Fairedge_dev/app_Vps/VpsBarMaker.py:lnx504'
    pysrc['gen_lowopen_data'] = f'/Fairedge_dev/app_QpsData/gen_lowopen_data.py:lnx504'
    pysrc['QpsCtpFeedMgr'] = f'/Fairedge_dev/app_Qps/QpsCtpFeedMgr.py:lnx504'
    pysrc['QpsCtpGtwMgr'] = f'/Fairedge_dev/app_Qps/QpsCtpGtwMgr.py:lnx504'
    pysrc['gen_fds_data'] = f'/Fairedge_dev/app_fds/gen_fds_data.py:lnx508'
    pysrc['do_gen_fds_data'] = f'/Fairedge_dev/app_fds/do_gen_fds_data.py:lnx508'
    pysrc['run_binanace_download_hist'] = f'/Fairedge_dev/3rd_binance/run_binanace_download_hist.py:lnx525'
    pysrc['gen_fdf_data'] = f'/Fairedge_dev/app_fdf_raw/gen_fdf_data.py:lnx515'
    pysrc['fdf_get_path'] = f'/Fairedge_dev/app_VpsTools/fdf_get_path.py:lnx515'
    # pysrc['download_stocks'] = f'/Fairedge_dev/app_fdf_raw/download_stocks.py:lnx515'
    pysrc['download_data'] = f'/Fairedge_dev/app_fdf_raw/download_data.py:lnx523'
    pysrc['do_gen_fdf_data'] = f"/Fairedge_dev/app_fdf_raw/do_gen_fdf_data.py:lnx516"
    pysrc['check_fdf_data'] = f"/Fairedge_dev/app_fdf_raw/check_fdf_data.py:lnx516"
    pysrc['gen_fdf_data_files_cfg'] = f"/Fairedge_dev/app_fdf_raw/gen_fdf_data_files_cfg.py:lnx516"
    pysrc['gen_pgan_low_open_data'] = f"/Fairedge_dev/app_fdf_raw/gen_pgan_low_open_data.py:lnx523"
    pysrc['GenFdfData_T'] = f"/Fairedge_dev/app_fdf_raw/GenFdfData_T.py:lnx525"
    pysrc['FdfTgtsSend'] = f"/Fairedge_dev/app_Vps/FdfTgtsSend.py:lnx525"
    pysrc['reprocess_fdf_data'] = f"/Fairedge_dev/app_fdf_raw/reprocess_fdf_data.py:lnx525"
    pysrc['VpsQuoteRecorder'] = f"/Fairedge_dev/app_Vps/VpsQuoteRecorder.py:lnx525"
    pysrc['get_tdx_tdata'] = f"/Fairedge_dev/app_fdf_raw/get_tdx_tdata.py:lnx525"
    pysrc['trading_books'] = f"/Fairedge_dev/app_common/trading_books.py:lnx525"
    pysrc['trading_books_postgres'] = f"/Fairedge_dev/app_common/trading_books_postgres.py:lnx532"
    pysrc['record_disk_speed'] = "/Fairedge_dev/app_QpsData/record_disk_speed.py:lnx525"
    pysrc['VpsPositionSplitter'] = "/Fairedge_dev/app_Vps/VpsPositionSplitter.py:lnx525"

    pysrc['load_fdf_info_to_postgres'] = "/Fairedge_dev/app_fdf/load_fdf_info_to_postgres.py:lnx531"
    pysrc['load_usetf_to_postgres'] = "/Fairedge_dev/app_QpsData/load_usetf_to_postgres.py:lnx531"
    pysrc['VpsPosInfoToPostgre'] = "/Fairedge_dev/app_Vps/VpsPosInfoToPostgre.py:lnx531"
    pysrc['rpyc_expr_server'] = "/Fairedge_dev/app_qpsrpyc/rpyc_expr_server.py:lnx531"
    pysrc['QDDownloaderPolygon'] = "/Fairedge_dev/app_QpsData_RQ/QDDownloaderPolygon.py:lnx531"
    pysrc['gen_us_stocks_data'] = "/Fairedge_dev/app_QpsData_RQ/gen_us_stocks_data.py:lnx531"
