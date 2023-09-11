package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type PrimaryKey struct {
	Name   string   `json:"name"`
	Fields []string `json:"fields"`
}

type ForeignKey struct {
	Name            string   `json:"name"`
	Fields          []string `json:"fields"`
	ReferenceTable  string   `json:"referenceTable"`
	ReferenceFields []string `json:"referenceFields"`
}

type TableField struct {
	Name         string `json:"name"`
	Type         string `json:"type"`
	Length       int    `json:"length"`
	Decimals     int    `json:"decimals"`
	IsNullable   bool   `json:"isNullable"`
	DefaultType  string `json:"defaultType"`
	DefaultValue string `json:"defaultValue"`
}

type Table struct {
	Name        string       `json:"name"`
	Fields      []TableField `json:"fields"`
	PrimaryKey  PrimaryKey   `json:"primaryKey"`
	ForeignKeys []ForeignKey `json:"foreignKeys"`
}

type Schema struct {
	Name   string  `json:"name"`
	Tables []Table `json:"tables"`
}

type Catalog struct {
	Name    string   `json:"name"`
	Schemas []Schema `json:"schemas"`
}

type Server struct {
	Catalogs []Catalog `json:"catalogs"`
}

type NDM2File struct {
	Server Server `json:"server"`
}

func Execute() error {
	if len(os.Args) != 3 {
		return fmt.Errorf("Usage: ndm2sql <inputFilePath> <outputFilePath>")
	}

	inputFilePath := os.Args[1]
	outputFilePath := os.Args[2]

	parsedData, err := parseFile(inputFilePath)
	if err != nil {
		return err
	}

	sql := generateSQLFromNDM2File(parsedData)
	err = saveToFile(sql, outputFilePath)
	if err != nil {
		return err
	}

	fmt.Printf("SQL saved to %s successfully.\n", outputFilePath)
	return nil
}

func generateCreateTableSQL(table Table) string {
	var fieldsSQL []string
	for _, field := range table.Fields {
		fieldSQL := fmt.Sprintf("%s %s", field.Name, field.Type)
		if field.Length != -2147483648 {
			fieldSQL += fmt.Sprintf("(%d", field.Length)
			if field.Decimals != -2147483648 {
				fieldSQL += fmt.Sprintf(",%d)", field.Decimals)
			} else {
				fieldSQL += ")"
			}
		}
		if !field.IsNullable {
			fieldSQL += " NOT NULL"
		}
		if field.DefaultType != "None" {
			defaultValueString := field.DefaultValue
			if field.DefaultType == "Expression" {
				defaultValueString = fmt.Sprintf("'%s'", field.DefaultValue)
			}
			fieldSQL += fmt.Sprintf(" DEFAULT %s", defaultValueString)
		}
		fieldsSQL = append(fieldsSQL, fieldSQL)
	}
	return fmt.Sprintf("CREATE TABLE %s (\n  %s\n);\n\n", table.Name, strings.Join(fieldsSQL, ",\n  "))
}

func generateCreatePrimaryKeySQL(table Table) string {
	return fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT pk_%s PRIMARY KEY (%s);\n\n", table.Name, table.Name, strings.Join(table.PrimaryKey.Fields, ", "))
}

func generateCreateForeignKeySQL(table Table) string {
	var sql strings.Builder
	for _, foreignKey := range table.ForeignKeys {
		fmt.Fprintf(&sql, "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s);\n\n", table.Name, foreignKey.Name, strings.Join(foreignKey.Fields, ", "), foreignKey.ReferenceTable, strings.Join(foreignKey.ReferenceFields, ", "))
	}
	return sql.String()
}

func generateCreateIndexSQL(table Table) string {
	return fmt.Sprintf("CREATE INDEX idx_%s_id ON %s (%s);\n\n", table.Name, table.Name, strings.Join(table.PrimaryKey.Fields, ", "))
}

func generateCreateIndexForForeignKeySQL(table Table) string {
	var sql strings.Builder
	for _, foreignKey := range table.ForeignKeys {
		fmt.Fprintf(&sql, "CREATE INDEX idx_fk_%s_%s ON %s (%s);\n\n", table.Name, foreignKey.ReferenceTable, table.Name, strings.Join(foreignKey.Fields, ", "))
	}
	return sql.String()
}

func generateSQLFromNDM2File(file NDM2File) string {
	var sql strings.Builder

	for _, catalog := range file.Server.Catalogs {
		for _, schema := range catalog.Schemas {
			tables := schema.Tables

			for _, table := range tables {
				sql.WriteString(generateCreateTableSQL(table))
			}

			for _, table := range tables {
				sql.WriteString(generateCreatePrimaryKeySQL(table))
			}

			for _, table := range tables {
				sql.WriteString(generateCreateForeignKeySQL(table))
			}

			for _, table := range tables {
				sql.WriteString(generateCreateIndexSQL(table))
			}

			for _, table := range tables {
				sql.WriteString(generateCreateIndexForForeignKeySQL(table))
			}
		}
	}
	return sql.String()
}

func parseFile(filePath string) (NDM2File, error) {
	var data NDM2File

	fileContent, err := os.ReadFile(filePath)
	if err != nil {
		return data, err
	}

	err = json.Unmarshal(fileContent, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}

func saveToFile(text string, filename string) error {
	return os.WriteFile(filename, []byte(text), 0644)
}
