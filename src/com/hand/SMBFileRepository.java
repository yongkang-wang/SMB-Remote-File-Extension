package com.hand;

import com.hierynomus.msdtyp.AccessMask;
import com.hierynomus.msfscc.fileinformation.FileIdBothDirectoryInformation;
import com.hierynomus.mssmb2.SMB2CreateDisposition;
import com.hierynomus.mssmb2.SMB2ShareAccess;
import com.hierynomus.protocol.commons.buffer.Buffer.BufferException;
import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.SmbConfig;
import com.hierynomus.smbj.auth.AuthenticationContext;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.DiskShare;
import com.hierynomus.smbj.share.File;
import com.thingworx.logging.LogUtilities;
import com.thingworx.metadata.FieldDefinition;
import com.thingworx.metadata.annotations.ThingworxBaseTemplateDefinition;
import com.thingworx.metadata.annotations.ThingworxConfigurationTableDefinition;
import com.thingworx.metadata.annotations.ThingworxConfigurationTableDefinitions;
import com.thingworx.metadata.annotations.ThingworxDataShapeDefinition;
import com.thingworx.metadata.annotations.ThingworxFieldDefinition;
import com.thingworx.metadata.annotations.ThingworxPropertyDefinition;
import com.thingworx.metadata.annotations.ThingworxPropertyDefinitions;
import com.thingworx.metadata.annotations.ThingworxServiceDefinition;
import com.thingworx.metadata.annotations.ThingworxServiceParameter;
import com.thingworx.metadata.annotations.ThingworxServiceResult;
import com.thingworx.things.Thing;
import com.thingworx.types.BaseTypes;
import com.thingworx.types.InfoTable;
import com.thingworx.types.collections.ValueCollection;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

/*
 * SMBFileRepository
 */
@ThingworxBaseTemplateDefinition(name = "GenericThing")
@ThingworxPropertyDefinitions(properties = {
		@ThingworxPropertyDefinition(name = "lastConnection", description = "Last connection time", category = "", baseType = "DATETIME", isLocalOnly = false, aspects = {
				"isReadOnly:true", "dataChangeType:VALUE" }),
		@ThingworxPropertyDefinition(name = "lastConnectionError", description = "Last connection error", category = "", baseType = "STRING", isLocalOnly = false, aspects = {
				"isReadOnly:true", "dataChangeType:VALUE" }) })
@ThingworxConfigurationTableDefinitions(tables = {
		@ThingworxConfigurationTableDefinition(name = "SMBConfiguration", description = "SMB Configuration", isMultiRow = false, ordinal = 0, dataShape = @ThingworxDataShapeDefinition(fields = {
				@ThingworxFieldDefinition(name = "hostname", description = "hostname", baseType = "STRING", ordinal = 0, aspects = {
						"isRequired:true", "defaultValue:0.0.0.0" }),
				@ThingworxFieldDefinition(name = "username", description = "username", baseType = "STRING", ordinal = 1, aspects = {
						"isRequired:true", "defaultValue:username" }),
				@ThingworxFieldDefinition(name = "password", description = "password", baseType = "PASSWORD", ordinal = 2, aspects = {
						"isRequired:true" }),
				@ThingworxFieldDefinition(name = "domain", description = "domain", baseType = "STRING", ordinal = 3),
				@ThingworxFieldDefinition(name = "shareName", description = "shareName", baseType = "STRING", ordinal = 4, aspects = {
						"isRequired:true" }) })),
		@ThingworxConfigurationTableDefinition(name = "DeviceConfiguration", description = "Device Configuration", isMultiRow = false, ordinal = 1, dataShape = @ThingworxDataShapeDefinition(fields = {
				@ThingworxFieldDefinition(name = "filePath", description = "filePath", baseType = "STRING", ordinal = 0),
				@ThingworxFieldDefinition(name = "fileType", description = "fileType", baseType = "STRING", ordinal = 1, aspects = {
						"isRequired:true", "selectOptions:TXT:TXT|CSV:CSV|XLS:XLS/XLSX|INI:INI|XML:XML|TX0:TX0" }) })),
		@ThingworxConfigurationTableDefinition(name = "BackupConfiguration", description = "Backup Configuration", isMultiRow = false, ordinal = 2, dataShape = @ThingworxDataShapeDefinition(fields = {
				@ThingworxFieldDefinition(name = "enable", description = "enable", baseType = "BOOLEAN", ordinal = 0, aspects = {
						"isRequired:true", "defaultValue:false" }),
				@ThingworxFieldDefinition(name = "path", description = "path", baseType = "STRING", ordinal = 1) })),
		@ThingworxConfigurationTableDefinition(name = "TimeoutConfiguration", description = "Timeout Configuration", isMultiRow = false, ordinal = 3, dataShape = @ThingworxDataShapeDefinition(fields = {
				@ThingworxFieldDefinition(name = "connectTimeout", description = "connectTimeout", baseType = "INTEGER", ordinal = 0, aspects = {
						"minimumValue:0.0", "isRequired:true", "defaultValue:120", "units:s", "maximumValue:600.0" }),
				@ThingworxFieldDefinition(name = "soTimeout", description = "soTimeout", baseType = "INTEGER", ordinal = 1, aspects = {
						"minimumValue:0.0", "isRequired:true", "defaultValue:180", "units:s",
						"maximumValue:600.0" }) })) })
public class SMBFileRepository extends Thing {

	private static Logger _logger = LogUtilities.getInstance().getApplicationLogger(SMBFileRepository.class);

	private static final long serialVersionUID = 1L;

	private String _hostname;
	private String _username;
	private String _password;
	private String _domain;
	private String _shareName;
	private String _filePath;
	private String _fileType;
	private Boolean _enable;
	private String _path;
	private Integer _connectTimeout;
	private Integer _soTimeout;

	@Override
	public void initializeThing() throws Exception {
		super.initializeThing();
		_hostname = (String) this.getConfigurationSetting("SMBConfiguration", "hostname");
		_username = (String) this.getConfigurationSetting("SMBConfiguration", "username");
		_password = (String) this.getConfigurationSetting("SMBConfiguration", "password");
		_domain = (String) this.getConfigurationSetting("SMBConfiguration", "domain");
		_shareName = (String) this.getConfigurationSetting("SMBConfiguration", "shareName");
		_filePath = (String) this.getConfigurationSetting("DeviceConfiguration", "filePath");
		_fileType = (String) this.getConfigurationSetting("DeviceConfiguration", "fileType");
		_enable = (Boolean) this.getConfigurationSetting("BackupConfiguration", "enable");
		_path = (String) this.getConfigurationSetting("BackupConfiguration", "path");
		_connectTimeout = (Integer) this.getConfigurationSetting("TimeoutConfiguration", "connectTimeout");
		_soTimeout = (Integer) this.getConfigurationSetting("TimeoutConfiguration", "soTimeout");
	}

	@ThingworxServiceDefinition(name = "ListFiles", description = "Get file system listing", category = "Files", isAllowOverride = false, aspects = {
			"isAsync:false" })
	@ThingworxServiceResult(name = "Result", description = "Result", baseType = "INFOTABLE", aspects = {
			"isEntityDataShape:true" })
	public InfoTable ListFiles() throws Exception {
		_logger.trace("Entering Service: ListFiles");
		InfoTable infoTable = new InfoTable();
		infoTable.addField(new FieldDefinition("name", BaseTypes.STRING));
		infoTable.addField(new FieldDefinition("creationTime", BaseTypes.DATETIME));
		infoTable.addField(new FieldDefinition("changeTime", BaseTypes.DATETIME));
		infoTable.addField(new FieldDefinition("lastWriteTime", BaseTypes.DATETIME));
		infoTable.addField(new FieldDefinition("lastAccessTime", BaseTypes.DATETIME));
		SmbConfig smbConfig = SmbConfig.builder().withTimeout(_connectTimeout, TimeUnit.SECONDS)
				.withSoTimeout(_soTimeout, TimeUnit.SECONDS).build();
		SMBClient smbClient = new SMBClient(smbConfig);
		Connection connection = smbClient.connect(_hostname);
		AuthenticationContext authenticationContext = new AuthenticationContext(_username, _password.toCharArray(),
				_domain);
		Session session = connection.authenticate(authenticationContext);
		DiskShare diskShare = (DiskShare) session.connectShare(_shareName);
		ValueCollection entry = new ValueCollection();
		for (FileIdBothDirectoryInformation fileInformation : diskShare.list(_filePath, "*." + _fileType)) {
			entry.clear();
			entry.SetStringValue("name", fileInformation.getFileName());
			entry.SetDateTimeValue("creationTime", fileInformation.getCreationTime().toDate());
			entry.SetDateTimeValue("changeTime", fileInformation.getChangeTime().toDate());
			entry.SetDateTimeValue("lastWriteTime", fileInformation.getLastWriteTime().toDate());
			entry.SetDateTimeValue("lastAccessTime", fileInformation.getLastAccessTime().toDate());
			infoTable.addRow(entry.clone());
		}
		smbClient.close();
		_logger.trace("Exiting Service: ListFiles");
		return infoTable;
	}

	@ThingworxServiceDefinition(name = "BackupFile", description = "Backup File", category = "Files", isAllowOverride = false, aspects = {
			"isAsync:false" })
	@ThingworxServiceResult(name = "Result", description = "", baseType = "NOTHING", aspects = {})
	public void BackupFile(
			@ThingworxServiceParameter(name = "fileName", description = "fileName", baseType = "STRING", aspects = {
					"isRequired:true" }) String fileName)
			throws Exception {
		_logger.trace("Entering Service: BackupFile");
		if (!_enable || _path.trim().length() == 0)
			return;
		SmbConfig smbConfig = SmbConfig.builder().withTimeout(_connectTimeout, TimeUnit.SECONDS)
				.withSoTimeout(_soTimeout, TimeUnit.SECONDS).build();
		SMBClient smbClient = new SMBClient(smbConfig);
		Connection connection = smbClient.connect(_hostname);
		AuthenticationContext authenticationContext = new AuthenticationContext(_username, _password.toCharArray(),
				_domain);
		Session session = connection.authenticate(authenticationContext);
		DiskShare diskShare = (DiskShare) session.connectShare(_shareName);
		File file = diskShare.openFile(_filePath + "\\" + fileName, EnumSet.of(AccessMask.GENERIC_READ), null,
				SMB2ShareAccess.ALL, SMB2CreateDisposition.FILE_OPEN, null);
		File copyFile = diskShare.openFile(_path + "\\" + file.getFileName(), EnumSet.of(AccessMask.GENERIC_WRITE),
				null, SMB2ShareAccess.ALL, SMB2CreateDisposition.FILE_OVERWRITE_IF, null);
		file.remoteCopyTo(copyFile);
		diskShare.rm(_filePath + "\\" + fileName);
		smbClient.close();
		_logger.trace("Exiting Service: BackupFile");
	}

	@ThingworxServiceDefinition(name = "CopyFile", description = "Copy a file", category = "Files", isAllowOverride = false, aspects = {
			"isAsync:false" })
	@ThingworxServiceResult(name = "Result", description = "", baseType = "NOTHING", aspects = {})
	public void CopyFile(
			@ThingworxServiceParameter(name = "fileName", description = "fileName", baseType = "STRING", aspects = {
					"isRequired:true" }) String fileName,
			@ThingworxServiceParameter(name = "path", description = "path", baseType = "STRING", aspects = {
					"isRequired:true" }) String path,
			@ThingworxServiceParameter(name = "newFileName", description = "newFileName", baseType = "STRING", aspects = {
					"isRequired:true" }) String newFileName)
			throws IOException, BufferException {
		_logger.trace("Entering Service: CopyFile");
		SmbConfig smbConfig = SmbConfig.builder().withTimeout(_connectTimeout, TimeUnit.SECONDS)
				.withSoTimeout(_soTimeout, TimeUnit.SECONDS).build();
		SMBClient smbClient = new SMBClient(smbConfig);
		Connection connection = smbClient.connect(_hostname);
		AuthenticationContext authenticationContext = new AuthenticationContext(_username, _password.toCharArray(),
				_domain);
		Session session = connection.authenticate(authenticationContext);
		DiskShare diskShare = (DiskShare) session.connectShare(_shareName);
		File file = diskShare.openFile(_filePath + "\\" + fileName, EnumSet.of(AccessMask.GENERIC_READ), null,
				SMB2ShareAccess.ALL, SMB2CreateDisposition.FILE_OPEN, null);
		File copyFile = diskShare.openFile(path + "\\" + newFileName, EnumSet.of(AccessMask.GENERIC_WRITE), null,
				SMB2ShareAccess.ALL, SMB2CreateDisposition.FILE_OVERWRITE_IF, null);
		file.remoteCopyTo(copyFile);
		smbClient.close();
		_logger.trace("Exiting Service: CopyFile");
	}

	@ThingworxServiceDefinition(name = "DeleteFile", description = "Delete a file", category = "Files", isAllowOverride = false, aspects = {
			"isAsync:false" })
	@ThingworxServiceResult(name = "Result", description = "Result", baseType = "NOTHING", aspects = {})
	public void DeleteFile(
			@ThingworxServiceParameter(name = "fileName", description = "fileName", baseType = "STRING", aspects = {
					"isRequired:true" }) String fileName)
			throws Exception {
		_logger.trace("Entering Service: DeleteFile");
		SmbConfig smbConfig = SmbConfig.builder().withTimeout(_connectTimeout, TimeUnit.SECONDS)
				.withSoTimeout(_soTimeout, TimeUnit.SECONDS).build();
		SMBClient smbClient = new SMBClient(smbConfig);
		Connection connection = smbClient.connect(_hostname);
		AuthenticationContext authenticationContext = new AuthenticationContext(_username, _password.toCharArray(),
				_domain);
		Session session = connection.authenticate(authenticationContext);
		DiskShare diskShare = (DiskShare) session.connectShare(_shareName);
		diskShare.rm(_filePath + "\\" + fileName);
		smbClient.close();
		_logger.trace("Exiting Service: DeleteFile");
	}

	@ThingworxServiceDefinition(name = "FileExists", description = "File Exists", category = "Files", isAllowOverride = false, aspects = {
			"isAsync:false" })
	@ThingworxServiceResult(name = "Result", description = "Result", baseType = "BOOLEAN", aspects = {})
	public Boolean FileExists(
			@ThingworxServiceParameter(name = "fileName", description = "fileName", baseType = "STRING", aspects = {
					"isRequired:true" }) String fileName)
			throws Exception {
		_logger.trace("Entering Service: fileExists");
		SmbConfig smbConfig = SmbConfig.builder().withTimeout(_connectTimeout, TimeUnit.SECONDS)
				.withSoTimeout(_soTimeout, TimeUnit.SECONDS).build();
		SMBClient smbClient = new SMBClient(smbConfig);
		Connection connection = smbClient.connect(_hostname);
		AuthenticationContext authenticationContext = new AuthenticationContext(_username, _password.toCharArray(),
				_domain);
		Session session = connection.authenticate(authenticationContext);
		DiskShare diskShare = (DiskShare) session.connectShare(_shareName);
		Boolean result = diskShare.fileExists(_filePath + "\\" + fileName);
		smbClient.close();
		_logger.trace("Exiting Service: fileExists");
		return result;
	}

	@ThingworxServiceDefinition(name = "LoadText", description = "Load Text from a file", category = "Files", isAllowOverride = false, aspects = {
			"isAsync:false" })
	@ThingworxServiceResult(name = "Content", description = "Content", baseType = "STRING", aspects = {})
	public String LoadText(
			@ThingworxServiceParameter(name = "fileName", description = "fileName", baseType = "STRING", aspects = {
					"isRequired:true" }) String fileName)
			throws Exception {
		_logger.trace("Entering Service: LoadText");
		if (_fileType.equals("XLS") || _fileType.equals("CSV")) {
			return "Invalid TXT";
		}
		SmbConfig smbConfig = SmbConfig.builder().withTimeout(_connectTimeout, TimeUnit.SECONDS)
				.withSoTimeout(_soTimeout, TimeUnit.SECONDS).build();
		SMBClient smbClient = new SMBClient(smbConfig);
		Connection connection = smbClient.connect(_hostname);
		AuthenticationContext authenticationContext = new AuthenticationContext(_username, _password.toCharArray(),
				_domain);
		Session session = connection.authenticate(authenticationContext);
		DiskShare diskShare = (DiskShare) session.connectShare(_shareName);
		File file = diskShare.openFile(_filePath + "\\" + fileName, EnumSet.of(AccessMask.GENERIC_READ), null,
				SMB2ShareAccess.ALL, SMB2CreateDisposition.FILE_OPEN, null);
		InputStream inputStream = file.getInputStream();
		BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "GBK"));
		StringBuilder result = new StringBuilder();
		String line = null;
		while ((line = reader.readLine()) != null) {
			result.append(line + "\r\n");
		}
		reader.close();
		smbClient.close();
		_logger.trace("Exiting Service: LoadText");
		return result.toString();
	}

	@ThingworxServiceDefinition(name = "LoadCSV", description = "Load CSV from a file", category = "Files", isAllowOverride = false, aspects = {
			"isAsync:false" })
	@ThingworxServiceResult(name = "Content", description = "Content", baseType = "INFOTABLE", aspects = {
			"isEntityDataShape:true" })
	public InfoTable LoadCSV(
			@ThingworxServiceParameter(name = "fileName", description = "fileName", baseType = "STRING", aspects = {
					"isRequired:true" }) String fileName)
			throws Exception {
		_logger.trace("Entering Service: LoadCSV");
		InfoTable infoTable = new InfoTable();
		if (!_fileType.equals("CSV")) {
			infoTable.addField(new FieldDefinition("LoadCSV Exception", BaseTypes.STRING));
			ValueCollection entry = new ValueCollection();
			entry.SetStringValue("LoadCSV Exception", "Invalid CSV");
			infoTable.addRow(entry.clone());
			return infoTable;
		}
		SmbConfig smbConfig = SmbConfig.builder().withTimeout(_connectTimeout, TimeUnit.SECONDS)
				.withSoTimeout(_soTimeout, TimeUnit.SECONDS).build();
		SMBClient smbClient = new SMBClient(smbConfig);
		Connection connection = smbClient.connect(_hostname);
		AuthenticationContext authenticationContext = new AuthenticationContext(_username, _password.toCharArray(),
				_domain);
		Session session = connection.authenticate(authenticationContext);
		DiskShare diskShare = (DiskShare) session.connectShare(_shareName);
		File file = diskShare.openFile(_filePath + "\\" + fileName, EnumSet.of(AccessMask.GENERIC_READ), null,
				SMB2ShareAccess.ALL, SMB2CreateDisposition.FILE_OPEN, null);
		InputStream inputStream = file.getInputStream();
		BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "GBK"));
		ValueCollection entry = new ValueCollection();
		String line = null;
		while ((line = reader.readLine()) != null) {
			String item[] = line.split(",");
			entry.clear();
			for (int i = 0; i < item.length; i++) {
				String columnName = "A" + String.valueOf(i + 1);
				if (!infoTable.hasField(columnName)) {
					infoTable.addField(new FieldDefinition(columnName, BaseTypes.STRING));
				}
				entry.SetStringValue(columnName, item[i]);
			}
			infoTable.addRow(entry.clone());
		}
		reader.close();
		smbClient.close();
		_logger.trace("Exiting Service: LoadCSV");
		return infoTable;
	}

	@ThingworxServiceDefinition(name = "LoadExcel", description = "Load Excel from a file", category = "Files", isAllowOverride = false, aspects = {
			"isAsync:false" })
	@ThingworxServiceResult(name = "Content", description = "Content", baseType = "INFOTABLE", aspects = {
			"isEntityDataShape:true" })
	public InfoTable LoadExcel(
			@ThingworxServiceParameter(name = "fileName", description = "fileName", baseType = "STRING", aspects = {
					"isRequired:true" }) String fileName)
			throws Exception {
		_logger.trace("Entering Service: LoadExcel");
		InfoTable infoTable = LoadExcel(fileName, 0, null);
		_logger.trace("Exiting Service: LoadExcel");
		return infoTable;
	}

	@ThingworxServiceDefinition(name = "LoadExcelForSheetIndex", description = "Load Excel from a file", category = "Files", isAllowOverride = false, aspects = {
			"isAsync:false" })
	@ThingworxServiceResult(name = "Content", description = "Content", baseType = "INFOTABLE", aspects = {
			"isEntityDataShape:true" })
	public InfoTable LoadExcelForSheetIndex(
			@ThingworxServiceParameter(name = "fileName", description = "fileName", baseType = "STRING", aspects = {
					"isRequired:true" }) String fileName,
			@ThingworxServiceParameter(name = "sheetIndex", description = "sheetIndex", baseType = "INTEGER", aspects = {
					"isRequired:true" }) Integer sheetIndex)
			throws Exception {
		_logger.trace("Entering Service: LoadExcelForSheetIndex");
		InfoTable infoTable = LoadExcel(fileName, sheetIndex, null);
		_logger.trace("Exiting Service: LoadExcelForSheetIndex");
		return infoTable;
	}

	@ThingworxServiceDefinition(name = "LoadExcelForSheetName", description = "Load Excel from a file", category = "Files", isAllowOverride = false, aspects = {
			"isAsync:false" })
	@ThingworxServiceResult(name = "Content", description = "Content", baseType = "INFOTABLE", aspects = {
			"isEntityDataShape:true" })
	public InfoTable LoadExcelForSheetName(
			@ThingworxServiceParameter(name = "fileName", description = "fileName", baseType = "STRING", aspects = {
					"isRequired:true" }) String fileName,
			@ThingworxServiceParameter(name = "sheetName", description = "sheetName", baseType = "STRING", aspects = {
					"isRequired:true" }) String sheetName)
			throws Exception {
		_logger.trace("Entering Service: LoadExcelForSheetName");
		InfoTable infoTable = LoadExcel(fileName, -1, sheetName);
		_logger.trace("Exiting Service: LoadExcelForSheetName");
		return infoTable;
	}

	private InfoTable LoadExcel(String fileName, Integer sheetIndex, String sheetName) throws Exception {
		InfoTable infoTable = new InfoTable();
		if (!_fileType.equals("XLS")) {
			infoTable.addField(new FieldDefinition("LoadExcel Exception", BaseTypes.STRING));
			ValueCollection entry = new ValueCollection();
			entry.SetStringValue("LoadExcel Exception", "Invalid Excel");
			infoTable.addRow(entry.clone());
			return infoTable;
		}
		List<String[]> list = new ArrayList<String[]>();
		SmbConfig smbConfig = SmbConfig.builder().withTimeout(_connectTimeout, TimeUnit.SECONDS)
				.withSoTimeout(_soTimeout, TimeUnit.SECONDS).build();
		SMBClient smbClient = new SMBClient(smbConfig);
		Connection connection = smbClient.connect(_hostname);
		AuthenticationContext authenticationContext = new AuthenticationContext(_username, _password.toCharArray(),
				_domain);
		Session session = connection.authenticate(authenticationContext);
		DiskShare diskShare = (DiskShare) session.connectShare(_shareName);
		File file = diskShare.openFile(_filePath + "\\" + fileName, EnumSet.of(AccessMask.GENERIC_READ), null,
				SMB2ShareAccess.ALL, SMB2CreateDisposition.FILE_OPEN, null);
		InputStream inputStream = file.getInputStream();
		Workbook workbook = null;
		if (file.getFileName().endsWith("xls")) {
			workbook = new HSSFWorkbook(inputStream);
		} else if (file.getFileName().endsWith("xlsx")) {
			workbook = new XSSFWorkbook(inputStream);
		}
		if (workbook != null) {
			Sheet sheet = null;
			if (sheetIndex < 0) {
				sheet = workbook.getSheet(sheetName);
			} else {
				sheet = workbook.getSheetAt(sheetIndex);
			}
			if (sheet == null) {
				smbClient.close();
				infoTable.addField(new FieldDefinition("LoadExcel Exception", BaseTypes.STRING));
				ValueCollection entry = new ValueCollection();
				entry.SetStringValue("LoadExcel Exception", "Sheet is null");
				infoTable.addRow(entry.clone());
				return infoTable;
			}
			int firstRowNum = sheet.getFirstRowNum();
			int lastRowNum = sheet.getLastRowNum();
			for (int rowNum = firstRowNum; rowNum <= lastRowNum; rowNum++) {
				Row row = sheet.getRow(rowNum);
				int firstCellNum = row.getFirstCellNum();
				int lastCellNum = row.getLastCellNum();
				String[] cells = new String[row.getLastCellNum()];
				for (int cellNum = firstCellNum; cellNum < lastCellNum; cellNum++) {
					Cell cell = row.getCell(cellNum);
					if (cell.getCellType() == CellType.STRING) {
						cells[cellNum] = cell.getStringCellValue();
					}
					if (cell.getCellType() == CellType.NUMERIC) {
						DecimalFormat format = new DecimalFormat();
						if (cell.getCellStyle().getDataFormatString().equals("General")) {
							format.applyPattern("0");
						}
						cells[cellNum] = format.format(cell.getNumericCellValue());
					}
				}
				list.add(cells);
			}
		}
		ValueCollection entry = new ValueCollection();
		for (int i = 0; i < list.size(); i++) {
			String[] line = list.get(i);
			entry.clear();
			for (int j = 0; j < line.length; j++) {
				String columnName = "A" + String.valueOf(j + 1);
				if (!infoTable.hasField(columnName)) {
					infoTable.addField(new FieldDefinition(columnName, BaseTypes.STRING));
				}
				entry.SetStringValue(columnName, line[j]);
			}
			infoTable.addRow(entry.clone());
		}
		smbClient.close();
		_logger.trace("Exiting Service: LoadExcelForSheetName");
		return infoTable;
	}

	@ThingworxServiceDefinition(name = "LoadExcelForMultiSheet", description = "Load Excel from a file", category = "Files", isAllowOverride = false, aspects = {
			"isAsync:false" })
	@ThingworxServiceResult(name = "Content", description = "Content", baseType = "INFOTABLE", aspects = {
			"isEntityDataShape:true" })
	public InfoTable LoadExcelForMultiSheet(
			@ThingworxServiceParameter(name = "fileName", description = "fileName", baseType = "STRING", aspects = {
					"isRequired:true" }) String fileName)
			throws Exception {
		_logger.trace("Entering Service: LoadExcelForMultiSheet");
		InfoTable infoTable = new InfoTable();
		if (!_fileType.equals("XLS")) {
			infoTable.addField(new FieldDefinition("LoadExcel Exception", BaseTypes.STRING));
			ValueCollection entry = new ValueCollection();
			entry.SetStringValue("LoadExcel Exception", "Invalid Excel");
			infoTable.addRow(entry.clone());
			return infoTable;
		}
		SmbConfig smbConfig = SmbConfig.builder().withTimeout(_connectTimeout, TimeUnit.SECONDS)
				.withSoTimeout(_soTimeout, TimeUnit.SECONDS).build();
		SMBClient smbClient = new SMBClient(smbConfig);
		Connection connection = smbClient.connect(_hostname);
		AuthenticationContext authenticationContext = new AuthenticationContext(_username, _password.toCharArray(),
				_domain);
		Session session = connection.authenticate(authenticationContext);
		DiskShare diskShare = (DiskShare) session.connectShare(_shareName);
		File file = diskShare.openFile(_filePath + "\\" + fileName, EnumSet.of(AccessMask.GENERIC_READ), null,
				SMB2ShareAccess.ALL, SMB2CreateDisposition.FILE_OPEN, null);
		InputStream inputStream = file.getInputStream();
		Workbook workbook = null;
		if (file.getFileName().endsWith("xls")) {
			workbook = new HSSFWorkbook(inputStream);
		} else if (file.getFileName().endsWith("xlsx")) {
			workbook = new XSSFWorkbook(inputStream);
		}
		if (workbook != null) {
			ValueCollection entry = new ValueCollection();
			for (int index = 0; index < workbook.getNumberOfSheets(); index++) {
				Sheet sheet = workbook.getSheetAt(index);
				if (sheet == null) {
					continue;
				}
				List<String[]> list = new ArrayList<String[]>();
				int firstRowNum = sheet.getFirstRowNum();
				int lastRowNum = sheet.getLastRowNum();
				for (int rowNum = firstRowNum; rowNum <= lastRowNum; rowNum++) {
					Row row = sheet.getRow(rowNum);
					int firstCellNum = row.getFirstCellNum();
					int lastCellNum = row.getLastCellNum();
					String[] cells = new String[row.getLastCellNum()];
					for (int cellNum = firstCellNum; cellNum < lastCellNum; cellNum++) {
						Cell cell = row.getCell(cellNum);
						if (cell.getCellType() == CellType.STRING) {
							cells[cellNum] = cell.getStringCellValue();
						}
						if (cell.getCellType() == CellType.NUMERIC) {
							DecimalFormat format = new DecimalFormat();
							if (cell.getCellStyle().getDataFormatString().equals("General")) {
								format.applyPattern("0");
							}
							cells[cellNum] = format.format(cell.getNumericCellValue());
						}
					}
					list.add(cells);
				}
				InfoTable sheeTable = new InfoTable();
				ValueCollection sheetCollection = new ValueCollection();
				for (int i = 0; i < list.size(); i++) {
					String[] line = list.get(i);
					sheetCollection.clear();
					for (int j = 0; j < line.length; j++) {
						String columnName = "A" + String.valueOf(j + 1);
						if (!sheeTable.hasField(columnName)) {
							sheeTable.addField(new FieldDefinition(columnName, BaseTypes.STRING));
						}
						sheetCollection.SetStringValue(columnName, line[j]);
					}
					sheeTable.addRow(sheetCollection.clone());
				}
				infoTable.addField(new FieldDefinition(sheet.getSheetName(), BaseTypes.INFOTABLE));
				entry.SetInfoTableValue(sheet.getSheetName(), sheeTable);
			}
			infoTable.addRow(entry.clone());
		}
		smbClient.close();
		_logger.trace("Exiting Service: LoadExcelForMultiSheet");
		return infoTable;
	}

	@ThingworxServiceDefinition(name = "LoadXML", description = "Load XML from a file", category = "Files", isAllowOverride = false, aspects = {
			"isAsync:false" })
	@ThingworxServiceResult(name = "Content", description = "Content", baseType = "XML", aspects = {})
	public Document LoadXML(
			@ThingworxServiceParameter(name = "fileName", description = "fileName", baseType = "STRING", aspects = {
					"isRequired:true" }) String fileName)
			throws IOException, ParserConfigurationException, SAXException {
		_logger.trace("Entering Service: LoadXML");
		if (!_fileType.equals("XML")) {
			return null;
		}
		SmbConfig smbConfig = SmbConfig.builder().withTimeout(_connectTimeout, TimeUnit.SECONDS)
				.withSoTimeout(_soTimeout, TimeUnit.SECONDS).build();
		SMBClient smbClient = new SMBClient(smbConfig);
		Connection connection = smbClient.connect(_hostname);
		AuthenticationContext authenticationContext = new AuthenticationContext(_username, _password.toCharArray(),
				_domain);
		Session session = connection.authenticate(authenticationContext);
		DiskShare diskShare = (DiskShare) session.connectShare(_shareName);
		File file = diskShare.openFile(_filePath + "\\" + fileName, EnumSet.of(AccessMask.GENERIC_READ), null,
				SMB2ShareAccess.ALL, SMB2CreateDisposition.FILE_OPEN, null);
		InputStream inputStream = file.getInputStream();
		DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
		Document document = documentBuilder.parse(inputStream);
		smbClient.close();
		_logger.trace("Exiting Service: LoadXML");
		return document;
	}

}
