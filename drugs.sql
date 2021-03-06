CREATE TABLE ATCTree
(
	Tree NVARCHAR(MAX) NOT NULL
);

CREATE TABLE Drugs
(
	Name NVARCHAR(127) NOT NULL,
	Link NVARCHAR(255) NOT NULL,
	Dosage NVARCHAR(255),
	Manufacture NVARCHAR(255),
	INN NVARCHAR(127),
	PharmGroup NVARCHAR(255),
	Registration NVARCHAR(127),
	ATCCode NVARCHAR(1023),
	Instruction NVARCHAR(MAX)
);
