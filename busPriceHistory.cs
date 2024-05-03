using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.IO;
using System.Linq;
using System.Net.Mail;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Westwind.BusinessObjects;
using Westwind.Tools;

namespace IOT.AVREFWebWebsite.Business
{
	public class busPriceHistory : busDataBase
	{
		public const string StandardFieldList = @"PartsData.p_sysid,
                                                        PartsData.p_descript,
                                                        PriceHistory.p_part_num,
                                                        PriceHistory.p_condition,
                                                        PriceHistory.p_type,
                                                        PriceHistory.p_update,
                                                        PriceHistory.p_disc,
                                                        PriceHistory.p_pkgs,
                                                        PriceHistory.p_deposit,
                                                        PriceHistory.p_unit,
                                                        PriceHistory.p_price";
		public const string StandardFromClause = @"PriceHistory
											LEFT JOIN ( SELECT Parts.p_sysid, PartInfo.p_descript, Parts.p_part_num, PartInfo.P_Condition, PartInfo.p_type
											FROM Parts
                                             LEFT JOIN PartInfo On Parts.p_sysid=PartInfo.p_sysid AND PartInfo.tLastUpdt is not NULL
											WHERE {0}
											) PartsData ON PriceHistory.p_part_num = PartsData.P_Part_Num AND PriceHistory.P_Condition = PartsData.P_Condition AND PriceHistory.p_type = PartsData.p_type";


		private static readonly object PriceHistoryUploadLock = new object();

		private string username = "";

		public string Username
		{
			get { return username; }
			set { username = value; }
		}

		private int nFetchSize = 1000;

		public int FetchSize
		{
			get { return this.nFetchSize; }
			set { this.nFetchSize = value; }
		}

		#region Constructor
		public busPriceHistory()
			: base()
		{
			this.Tablename = "PriceHistory";
			this.PkField = "PriceHistoryID";
			this.PkType = PkFieldTypes.intType;
		}
		#endregion

		#region Functions

		public bool LoadPriceHistory(string part_num = null, string type = null, string condition = null, bool removeNonDigits = false)
		{
			bool retVal = false;

			#region Populate conditions/parameters
			List<string> whereConditions = new List<string>();
			List<string> innerWhereConditions = new List<string>();
			List<IDbDataParameter> parameters = new List<IDbDataParameter>();

			StringBuilder sb = new StringBuilder();
			foreach (char cCurrent in part_num)
			{
				if (char.IsLetterOrDigit(cCurrent))
				{
					sb.Append(cCurrent);
				}
			}
			part_num = sb.ToString();

			if (!string.IsNullOrWhiteSpace(part_num))
			{
				//whereConditions.Add("PriceHistory.p_part_num LIK ?PartNum%"); not needed since we are doing a broader search on parts that might not have the same part num as history table
				// innerWhereConditions.Add("Parts.p_part_num_no_dash LIKE ?PartNum");
				innerWhereConditions.Add("Parts.p_part_num_no_dash = ?PartNum");
				parameters.Add(this.CreateParameter("?PartNum", $"{part_num}%"));
			}
			if (!string.IsNullOrWhiteSpace(type))
			{
				whereConditions.Add("PriceHistory.p_type = ?Type");
				innerWhereConditions.Add("PartInfo.p_type = ?Type");
				parameters.Add(this.CreateParameter("?Type", type));
			}
			if (!string.IsNullOrWhiteSpace(condition))
			{
				whereConditions.Add("PriceHistory.p_condition = ?Condition");
				innerWhereConditions.Add("PartInfo.p_condition = ?Condition");
				parameters.Add(this.CreateParameter("?Condition", condition));
			}
			#endregion

			string whereClause = string.Join(" AND ", whereConditions);
			if (string.IsNullOrWhiteSpace(whereClause))
				whereClause = "1=1";


			string firstInnerWhereClause = string.Join(" AND ", innerWhereConditions);
			string secondInnerWhereClause = firstInnerWhereClause;
			if (string.IsNullOrWhiteSpace(firstInnerWhereClause))
			{
				firstInnerWhereClause = "1=1";
				secondInnerWhereClause = "1=1";
			}
			else
			{
				// on the live data this was taking forever to load, lets hope there's not more than 2000 on this inner join
				firstInnerWhereClause = firstInnerWhereClause + " LIMIT 2000";
			}

			//Updated this to group by and to get part history for part numbers excluding punctuation.
			string command = string.Format(@"SELECT {0} 
                                                      FROM {1}                 
                                                      WHERE PriceHistory.p_part_num IN ( SELECT Parts.p_part_num
																						FROM Parts
																						LEFT JOIN PartInfo On Parts.p_sysid=PartInfo.p_sysid AND PartInfo.tLastUpdt is not NULL
																						WHERE {2}
																						)
													  AND {3}
													  GROUP BY PartsData.p_descript,
                                                        PriceHistory.p_part_num,
                                                        PriceHistory.p_condition,
                                                        PriceHistory.p_type,
                                                        PriceHistory.p_update,
                                                        PriceHistory.p_disc,
                                                        PriceHistory.p_pkgs,
                                                        PriceHistory.p_deposit,
                                                        PriceHistory.p_unit,
                                                        PriceHistory.p_price
													ORDER BY PriceHistory.p_part_num ASC, PriceHistory.p_update DESC", this.SelectFieldsWithUserPreferences(this.Username), string.Format(StandardFromClause, firstInnerWhereClause), secondInnerWhereClause, whereClause);

			if (this.ExecuteWithErrorLog(command, this.Tablename, parameters.ToArray()) < 1)
			{
				this.SetError("Price history not found.");
				retVal = false;
			}
			else
			{
				this.DataRow = this.DataSet.Tables[this.Tablename].Rows[0];
				retVal = true;
			}

			return retVal;
		}


		public void ImplementNewHistoryFile(string priceHistoryDBF, Guid userID, string strMailSenderAccount, string strErrorEmails)
		{
			var oUsers = WebStoreFactory.GetBusUsers();
			oUsers.LoadByUserID(userID);
			var emailAddress = oUsers.DataRow.Field<string>("Email");

			DataTable dtNewPriceHistory = new DataTable("PriceHistory");
			using (OleDbConnection foxCon = new OleDbConnection("Provider=vfpoledb.1;Data Source=" + priceHistoryDBF + ";Collating Sequence=general"))
			using (OleDbCommand foxCommand = new OleDbCommand("SELECT * FROM " + priceHistoryDBF, foxCon))
			using (OleDbDataAdapter daFox = new OleDbDataAdapter(foxCommand))
			{
				daFox.Fill(dtNewPriceHistory);
			}

			if (dtNewPriceHistory.AsEnumerable().Any())
			{
				dtNewPriceHistory.Columns["p_update"].ColumnName = "p_update_original";
				dtNewPriceHistory.Columns.Add("p_update", typeof(string));

				Parallel.ForEach(dtNewPriceHistory.AsEnumerable(), dr => dr.SetField("p_update", dr.Field<DateTime>("p_update_original").ToString("yyyy-MM-dd")));
				//foreach (DataRow dr in dtNewPriceHistory.Rows)
				//{
				//	dr.SetField("p_update", dr.Field<DateTime>("p_update_original").ToString("yyyy-MM-dd"));
				//}
				lock (PriceHistoryUploadLock)
				{
					var tempFile = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
					try
					{
						IOT.Main.Data.CSV.Export(dtNewPriceHistory, tempFile, false, "^^,",
							dtNewPriceHistory.Columns["p_part_num"],
							dtNewPriceHistory.Columns["p_type"],
							dtNewPriceHistory.Columns["p_price"],
							dtNewPriceHistory.Columns["p_disc"],
							dtNewPriceHistory.Columns["p_deposit"],
							dtNewPriceHistory.Columns["p_condit"],
							dtNewPriceHistory.Columns["p_unit"],
							dtNewPriceHistory.Columns["p_update"],
							dtNewPriceHistory.Columns["p_pkgs"]);
						this.ExecuteNonQueryWithErrorLog(@"CREATE TEMPORARY TABLE IF NOT EXISTS PriceHistoryTemp (p_part_num varchar(30) DEFAULT NULL,
															p_type varchar(3) DEFAULT NULL,
															p_price varchar(10) DEFAULT NULL,
															p_disc varchar(2) DEFAULT NULL,
															p_deposit varchar(8) DEFAULT NULL,
															p_condition varchar(2) DEFAULT NULL,
															p_unit varchar(3) DEFAULT NULL,
															p_update date DEFAULT NULL,
															p_pkgs varchar(4))");
						this.ExecuteNonQueryWithErrorLog(@"TRUNCATE TABLE PriceHistoryTemp");
						this.ExecuteNonQueryWithErrorLog(string.Format("Load Data Local InFile '{0}' INTO Table PriceHistoryTemp FIELDS TERMINATED BY '^^,' LINES TERMINATED BY '{1}'", tempFile.Replace("\\", "\\\\"), Environment.NewLine));
						this.ExecuteNonQueryWithErrorLog(@"INSERT INTO PriceHistory (p_part_num, p_type, p_price, p_disc, p_deposit, p_condition, p_unit, p_update, p_pkgs)
														SELECT PriceHistoryTemp.p_part_num, PriceHistoryTemp.p_type, PriceHistoryTemp.p_price, PriceHistoryTemp.p_disc, PriceHistoryTemp.p_deposit, PriceHistoryTemp.p_condition, PriceHistoryTemp.p_unit, PriceHistoryTemp.p_update, PriceHistoryTemp.p_pkgs
														FROM PriceHistoryTemp
														LEFT JOIN PriceHistory ExistingHistory 
														ON ExistingHistory.p_part_num = PriceHistoryTemp.p_part_num 
														AND ExistingHistory.p_type = PriceHistoryTemp.p_type 
														AND ExistingHistory.p_price = PriceHistoryTemp.p_price 
														AND ExistingHistory.p_condition = PriceHistoryTemp.p_condition 
														AND ExistingHistory.p_update = PriceHistoryTemp.p_update
														WHERE ExistingHistory.PriceHistoryID IS NULL");


						var hasError = this.Error;
						var errorMessage = this.ErrorMessage;

						if (hasError)
							this.SendErrorEmail(strMailSenderAccount, emailAddress, strErrorEmails, priceHistoryDBF, errorMessage);
						else
							this.SendEmail(strMailSenderAccount, emailAddress, priceHistoryDBF);
					}
					catch (Exception ex)
					{
						Logger.LogError(ex);
						this.SendErrorEmail(strMailSenderAccount, emailAddress, strErrorEmails, priceHistoryDBF, ex.Message);
					}
					finally
					{
						try
						{
							this.ExecuteNonQueryWithErrorLog("DROP TEMPORARY TABLE IF EXISTS PriceHistoryTemp");
						}
						catch { }
						try
						{
							File.Delete(tempFile);
						}
						catch { }
					}
				}
			}
		}


		public void SendEmail(string strMailSenderAccount, string strRecipient, string strFile)
		{
			SmtpClient smtpMail = new SmtpClient();

			smtpMail.Send(strMailSenderAccount, strRecipient, String.Format("AvrefWeb Pricing History Queue Has Finished Processing File: {0}", strFile), String.Format("This is a notification to inform you that the AvrefWeb pricing history queue has finished processing file {0}.  You may log onto the AVREF website to view the results.", strFile + Environment.NewLine));
		}

		public void SendErrorEmail(string strMailSenderAccount, string strStandardRecipients, string strErrorEmails, string strFile, string strError)
		{
			SmtpClient smtpMail = new SmtpClient();
			string[] errorrecipients = strErrorEmails.Split(new string[] { ";" }, StringSplitOptions.RemoveEmptyEntries);
			string[] standardrecipients = strStandardRecipients.Split(new string[] { ";" }, StringSplitOptions.RemoveEmptyEntries);

			foreach (string strRecipient in errorrecipients)
				smtpMail.Send(strMailSenderAccount, strRecipient, String.Format("AvrefWeb Pricing History Queue Had Trouble Processing File: {0}", strFile), String.Format("This is a notification to inform you that the AvrefWeb pricing history queue could not process file {0}.{1} It encountered the following error: {2}", strFile, Environment.NewLine, strError));

			foreach (string strRecipient in standardrecipients)
				smtpMail.Send(strMailSenderAccount, strRecipient, String.Format("AvrefWeb Pricing History Queue Had Trouble Processing File: {0}", strFile), String.Format("This is a notification to inform you that the AvrefWeb pricing history queue could not process file {0}.{1}{1}Please review the file format before trying again. If this problem persists contact {2}", strFile, Environment.NewLine, errorrecipients[0]));
		}

		#endregion


		#region Select Fields With User Preferences
		/// <summary>
		/// Adds the appropriate formated fields to the select statement based on the users preferences.
		/// </summary>
		/// <param name="username">Username to retrieve the preferences for</param>
		/// <returns>The list of fields to be selected</returns>
		protected string SelectFieldsWithUserPreferences(string username)
		{
			busUserPreferences oUserPrefs = WebStoreFactory.GetBusUserPreferences();
			oUserPrefs.LoadByUserName(username);

			//Populate fetchsize property
			if (oUserPrefs.DataRow["FetchSize"] != DBNull.Value)
				this.FetchSize = Convert.ToInt32(oUserPrefs.DataRow["FetchSize"]);

			string formatExpression = "CAST({0} AS Decimal(20,2))*" + Convert.ToString(oUserPrefs.DataRow["ConversionFactor"], new System.Globalization.CultureInfo("en-US"));
			string selectFields = StandardFieldList;

			formatExpression = "ROUND(" + formatExpression + ",2)";

			if ((sbyte)oUserPrefs.DataRow["CurrencyJustification"] == 1)
				formatExpression = "CAST(CONCAT('" + oUserPrefs.DataRow["CurrencySymbol"].ToString().Replace("'", @"\'") + "'," + formatExpression + ") as char(30))";
			else
				formatExpression = "CAST(CONCAT(" + formatExpression + ",'" + oUserPrefs.DataRow["CurrencySymbol"].ToString().Replace("'", @"\'") + "') as char(30))";

			selectFields += "," + string.Format(formatExpression, "PriceHistory.p_price") + " as formatted_p_price";
			selectFields += "," + string.Format(formatExpression, "PriceHistory.p_deposit") + " as formatted_p_deposit";


			return selectFields;
		}


		#endregion
	}
}