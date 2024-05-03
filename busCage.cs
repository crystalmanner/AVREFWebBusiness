using System;
using System.Collections;
using System.Data;
using System.Data.OleDb;
using System.Net.Mail;
using System.Text;
using Westwind.BusinessObjects;
using Westwind.Tools;

namespace IOT.AVREFWebWebsite.Business
{
    public class busCage : busDataBase
    {
        public const string StandardFieldList = @"Cage1.cage,
                                                        Cage1.adr_1,
                                                        Cage1.adr_11,
                                                        Cage1.adr_12,
                                                        Cage1.adr_13,
                                                        Cage1.adr_14,
                                                        Cage1.adr_15,
                                                        Cage1.adr_16,
                                                        Cage1.adr_17,
                                                        Cage1.adr_18,
                                                        Cage1.adr_2,
                                                        Cage1.adr_3,
                                                        Cage1.adr_4,
                                                        Cage1.adr_5,
                                                        Cage2.city_d,
                                                        Cage2.city_f,
                                                        Cage2.country_d,
                                                        Cage2.country_f,
                                                        Cage2.post_zone,
                                                        Cage2.po_box_d,
                                                        Cage2.po_box_f,
                                                        Cage2.province,
                                                        Cage2.state_d,
                                                        Cage2.street_1_d,
                                                        Cage2.street_1_f,
                                                        Cage2.street_2_d,
                                                        Cage2.street_2_f,
                                                        Cage2.zip_code,
                                                        Cage3.adp_pnt,
                                                        Cage3.affil,
                                                        Cage3.assoc_cage,
                                                        Cage3.cao,
                                                        Cage3.fax_d,
                                                        Cage3.fax_f,
                                                        Cage3.fips_city,
                                                        Cage3.fips_cnty,
                                                        Cage3.fips_state,
                                                        Cage3.nscm_desc,
                                                        Cage3.org_id_txt,
                                                        Cage3.phone_d,
                                                        Cage3.phone_f,
                                                        Cage3.prim_bus,
                                                        Cage3.rep_cage,
                                                        Cage3.sic_1,
                                                        Cage3.sic_2,
                                                        Cage3.sic_3,
                                                        Cage3.sic_4,
                                                        Cage3.size,
                                                        Cage3.status,
                                                        Cage3.type,
                                                        Cage3.type_bus,
                                                        Cage3.woman_own";
        public const string StandardFromClause = @"Cage1               
                                                INNER JOIN Cage2 On Cage1.cage=Cage2.cage AND Cage2.tLastUpdt is not NULL
                                                INNER JOIN Cage3 On Cage1.cage=Cage3.cage AND Cage3.tLastUpdt is not NULL";

        public const string SearchFieldList = @"Cage1.cage as p_sysid,
                                                '' as p_part_num,
                                                '' as p_descript,
                                                '' as p_condition,
                                                '' as p_part_name,
                                                '0' as formatted_p_price,
                                                '0' as formatted_p_deposit,
                                                '0' as p_disc,
                                                '0' as formatted_discounted_price,
                                                CURDATE() as p_update,
                                                '' as p_nsn,
                                                Cage1.cage as p_cage,
                                                Cage1.adr_1 as p_source,
                                                Cage1.adr_1 as r_name,
                                                '' as r_phone,
                                                Cage3.status as p_status,
                                                '0' as nAltPartCount,
                                                Cage1.Cage as nsn,
                                                '' as MCRLID,
                                                '' as MDISID,
                                                Cage1.adr_1 as item_nam,
                                                '' as ref_numb,
                                                Cage1.cage as cage_cd_92,
                                                Cage1.adr_1 as dup_isc,
                                                Cage3.status as rncc,
                                                '' as rnvc,
                                                '' as hcc,
                                                '' as msds,
                                                '' as sadc,
                                                '' as dup_da,
                                                '' as hmic,
                                                '' as inc_code,
                                                null as cage,
                                                null as contract,
                                                null as qty,
                                                null as uom,
                                                null as formatted_unit_price,
                                                null as formatted_total,
                                                CurDate() as date,
                                                '' as p_group_num,
                                                '' as p_model,
                                                '' as p_comment,
                                                0 as p_price,
                                                '' as p_type,
                                                '' as p_pkgs,
                                                '' as p_deposit,
                                                '' as p_unit,
                                                '' as p_sscd,
                                                0 as price2,
                                                Cage1.cage,
                                                Cage1.adr_1,
                                                Cage1.adr_11,
                                                Cage1.adr_12,
                                                Cage1.adr_13,
                                                Cage1.adr_14,
                                                Cage1.adr_15,
                                                Cage1.adr_16,
                                                Cage1.adr_17,
                                                Cage1.adr_18,
                                                Cage1.adr_2,
                                                Cage1.adr_3,
                                                Cage1.adr_4,
                                                Cage1.adr_5,
                                                Cage3.fax_d,
                                                Cage3.fax_f,
                                                Cage3.phone_d,
                                                Cage3.phone_f,
                                                Cage2.city_d,
                                                Cage2.city_f,
                                                Cage2.country_d,
                                                Cage2.country_f,
                                                Cage2.post_zone,
                                                Cage2.po_box_d,
                                                Cage2.po_box_f,
                                                Cage2.province,
                                                Cage2.state_d,
                                                Cage2.street_1_d,
                                                Cage2.street_1_f,
                                                Cage2.street_2_d,
                                                Cage2.street_2_f,
                                                Cage2.zip_code";

        #region Constructor
        public busCage()
            : base()
        {
            this.Tablename = "Cage1";
            this.PkField = "iPrimary";
            this.PkType = PkFieldTypes.intType;
        }

        /// <summary>
        /// Currently used by the Windows service
        /// </summary>
        /// <param name="strConnectionString"></param>
        public busCage(string strConnectionString)
            : this()
        {
            this.ConnectType = ServerTypes.MySql;
            this.ConnectionString = strConnectionString;
        }
        #endregion

        #region DataSets

        #region Load Data
        public bool GetEmptyDataSet()
        {
            IDbCommand Command = this.CreateCommand(string.Format(@"SELECT {0}                                                      
                                                      FROM {1}
                                                      WHERE false", busCage.StandardFieldList, busCage.StandardFromClause));

            return this.ExecuteWithErrorLog(Command, this.Tablename) == 0;

        }

        public bool LoadByCage(string strCage, string strTableName)
        {
            bool retVal = false;

            IDbCommand Command = this.CreateCommand(string.Format(@"SELECT {0} 
                                                      FROM {1}                 
                                                      WHERE Cage1.cage=?Cage AND Cage1.tLastUpdt is not NULL", busCage.StandardFieldList, busCage.StandardFromClause));
            Command.Parameters.Add(this.CreateParameter("?Cage", strCage));

            if (this.ExecuteWithErrorLog(Command, strTableName) < 1)
            {
                this.SetError("Cage not found.");
                retVal = false;
            }
            else
            {
                this.DataRow = this.DataSet.Tables[strTableName].Rows[0];
                retVal = true;
            }

            return retVal;
        }
        public bool LoadByCage(string strCage)
        {
            return this.LoadByCage(strCage, this.DefaultViewName);
        }

        public bool LoadAllCages()
        {
            IDbCommand Command = this.CreateCommand(string.Format(@"SELECT {0}
                                                      FROM {1}
                                                        WHERE Cage1.tLastUpdt is not NULL", busCage.StandardFieldList, busCage.StandardFromClause));

            if (this.ExecuteWithErrorLog(Command, this.Tablename) < 1)
                this.SetError("Cage not found.");
            else
            {
                this.DataRow = this.DataSet.Tables[this.Tablename].Rows[0];
                return true;
            }

            return false;
        }

        #endregion

        #region Searches
        public bool SearchParts(string valueToFind, SearchTypes typeOfSearch, bool exactMatch, bool removeHyphens, string strUserName)
        {
            busUserPreferences oUserPrefs = WebStoreFactory.GetBusUserPreferences();

            string whereClause = "";
            //string strAltPartCountClause = String.Empty;
            exactMatch = false;
            removeHyphens = true;

            string fieldExpression = "REPLACE(REPLACE(Cage1.Cage, '-', ''), ' ', '')";
            string comparisonOperator;
            string searchType = "Cage";

            switch (typeOfSearch)
            {

                case SearchTypes.Manufacturer:
                    fieldExpression = "REPLACE(REPLACE(Cage1.adr_1, '-', ''), ' ', '')";
                    searchType = "Company";
                    break;
                case SearchTypes.CAGE:
                    fieldExpression = "REPLACE(REPLACE(Cage1.Cage, '-', ''), ' ', '')";
                    searchType = "Cage";
                    break;
            }

            oUserPrefs.LoadByUserName(strUserName);

            StringBuilder sb = new StringBuilder();
            foreach (char cCurrent in valueToFind)
            {
                if (char.IsLetterOrDigit(cCurrent))
                {
                    sb.Append(cCurrent);
                }
            }
            valueToFind = sb.ToString();

            if (exactMatch)
            {
                comparisonOperator = "=";
            }
            else
            {
                comparisonOperator = "LIKE";
                valueToFind += "%";
            }

            whereClause += string.Format(" {0} {1} ?valueToFind", fieldExpression, comparisonOperator);
            //strAltPartCountClause = @"LEFT JOIN (SELECT Parts.p_group_num, COUNT(Parts.p_group_num) as nAltPartCount FROM Parts INNER JOIN PartInfo On Parts.p_sysid=PartInfo.p_sysid AND PartInfo.tLastUpdt is not NULL INNER JOIN Resource On Parts.p_sysid=Resource.p_sysid AND Resource.tLastUpdt is not NULL WHERE " + whereClause + " GROUP BY Parts.p_group_num) P1 ON P1.p_group_num = Parts.p_group_num";
            IDbCommand Command = this.CreateCommand(string.Format(@"SELECT {0}
                                                      FROM {1}
                                                      WHERE ({2}) LIMIT {3}", busCage.SearchFieldList, busCage.StandardFromClause, whereClause, (oUserPrefs.DataRow["FetchSize"] != DBNull.Value) ? Convert.ToInt32(oUserPrefs.DataRow["FetchSize"]).ToString() : "1000"));

            Command.Parameters.Add(this.CreateParameter("?valueToFind", valueToFind));
            Command.Prepare();

            if (this.ExecuteWithErrorLog(Command, this.Tablename) < 1)
                this.SetError(searchType + " not found.");
            else
            {
                this.DataRow = this.DataSet.Tables[this.Tablename].Rows[0];
                return true;
            }

            return false;

        }
        #endregion

        #endregion

        #region Functions

        #endregion


    }
}