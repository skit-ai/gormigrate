package gormigrate

import (
	"fmt"

	e "github.com/Vernacular-ai/errors"
	"github.com/Vernacular-ai/gorm"
)

const (
	initSchemaMigrationID = "SCHEMA_INIT"
)

// MigrateFunc is the func signature for migrating.
type MigrateFunc func(*gorm.DB) error

// RollbackFunc is the func signature for rollbacking.
type RollbackFunc func(*gorm.DB) error

// InitSchemaFunc is the func signature for initializing the schema.
type InitSchemaFunc func(*gorm.DB) error

// Options define options for all migrations.
type Options struct {
	// TableName is the migration table.
	TableName string
	// IDColumnName is the name of column where the migration id will be stored.
	IDColumnName string
	// IDColumnSize is the length of the migration id column
	IDColumnSize int
	// UseTransaction makes Gormigrate execute migrations inside a single transaction.
	// Keep in mind that not all databases support DDL commands inside transactions.
	UseTransaction bool
	// ValidateUnknownMigrations will cause migrate to fail if there's unknown migration
	// IDs in the database
	ValidateUnknownMigrations bool
}

// Migration represents a database migration (a modification to be made on the database).
type Migration struct {
	// ID is the migration identifier. Usually a timestamp like "201601021504".
	ID string
	// Migrate is a function that will br executed while running this migration.
	Migrate MigrateFunc
	// Rollback will be executed on rollback. Can be nil.
	Rollback RollbackFunc
}

// Gormigrate represents a collection of all migrations of a database schema.
type Gormigrate struct {
	db         *gorm.DB
	tx         *gorm.DB
	options    *Options
	migrations []*Migration
	initSchema InitSchemaFunc
}

var (
	// DefaultOptions can be used if you don't want to think about options.
	DefaultOptions = &Options{
		TableName:                 "migrations",
		IDColumnName:              "id",
		IDColumnSize:              255,
		UseTransaction:            false,
		ValidateUnknownMigrations: false,
	}

	// ErrRollbackImpossible() is returned when trying to rollback a migration
	// that has no rollback function.
	ErrRollbackImpossible = func() error { return e.NewError("gormigrate: It's impossible to rollback this migration") }

	// ErrNoMigrationDefined() is returned when no migration is defined.
	ErrNoMigrationDefined = func() error { return e.NewError("gormigrate: No migration defined") }

	// ErrMissingID() is returned when the ID od migration is equal to ""
	ErrMissingID = func() error { return e.NewError("gormigrate: Missing ID in migration") }

	// ErrNoRunMigration()() is returned when any run migration was found while
	// running RollbackLast
	ErrNoRunMigration = func() error { return e.NewError("gormigrate: Could not find last run migration") }

	// ErrMigrationIDDoesNotExist() is returned when migrating or rolling back to a migration ID that
	// does not exist in the list of migrations
	ErrMigrationIDDoesNotExist = func() error { return e.NewError("gormigrate: Tried to migrate to an ID that doesn't exist") }

	// ErrUnknownPastMigration() is returned if a migration exists in the DB that doesn't exist in the code
	ErrUnknownPastMigration = func() error { return e.NewError("gormigrate: Found migration in DB that does not exist in code") }

	// ErrUnknownPastMigration() is returned if a migration exists in the DB that doesn't exist in the code
	ReservedIDError = func(id string) error { return e.NewError(`gormigrate: Reserved migration ID: "%s"`, id) }

	// ErrUnknownPastMigration() is returned if a migration exists in the DB that doesn't exist in the code
	DuplicatedIDError = func(id string) error { return e.NewError(`gormigrate: Duplicated migration ID: "%s"`, id) }
)

// New returns a new Gormigrate.
func New(db *gorm.DB, options *Options, migrations []*Migration) *Gormigrate {
	if options.TableName == "" {
		options.TableName = DefaultOptions.TableName
	}
	if options.IDColumnName == "" {
		options.IDColumnName = DefaultOptions.IDColumnName
	}
	if options.IDColumnSize == 0 {
		options.IDColumnSize = DefaultOptions.IDColumnSize
	}
	return &Gormigrate{
		db:         db,
		options:    options,
		migrations: migrations,
	}
}

// InitSchema sets a function that is run if no migration is found.
// The idea is preventing to run all migrations when a new clean database
// is being migrating. In this function you should create all tables and
// foreign key necessary to your application.
func (g *Gormigrate) InitSchema(initSchema InitSchemaFunc) {
	g.initSchema = initSchema
}

// Migrate executes all migrations that did not run yet.
func (g *Gormigrate) Migrate() error {
	if !g.hasMigrations() {
		return ErrNoMigrationDefined()
	}
	var targetMigrationID string
	if len(g.migrations) > 0 {
		targetMigrationID = g.migrations[len(g.migrations)-1].ID
	}
	if err := g.migrate(targetMigrationID); err != nil {
		return e.Chain(err, "Unable to migrate to %s", targetMigrationID)
	}

	return nil
}

// MigrateTo executes all migrations that did not run yet up to the migration that matches `migrationID`.
func (g *Gormigrate) MigrateTo(migrationID string) error {
	if err := g.checkIDExist(migrationID); err != nil {
		return e.Chain(err, "Unable to migrate to %s", migrationID)
	}

	if err := g.migrate(migrationID); err != nil {
		return e.Chain(err, "Unable to migrate to %s", migrationID)
	}

	return nil
}

func (g *Gormigrate) migrate(migrationID string) error {
	if !g.hasMigrations() {
		return ErrNoMigrationDefined()
	}

	if err := g.checkReservedID(); err != nil {
		return e.Chain(err, "Unable to migrate")
	}

	if err := g.checkDuplicatedID(); err != nil {
		return e.Chain(err, "Unable to migrate")
	}

	g.begin()
	defer g.rollback()

	if err := g.createMigrationTableIfNotExists(); err != nil {
		return e.Chain(err, "Unable to create migration table if it doesn't exist")
	}

	if g.options.ValidateUnknownMigrations {
		unknownMigrations, err := g.unknownMigrationsHaveHappened()
		if err != nil {
			return e.Chain(err, "Unable to fetch unknown migrations")
		}
		if unknownMigrations {
			return ErrUnknownPastMigration()
		}
	}

	if g.initSchema != nil {
		canInitializeSchema, err := g.canInitializeSchema()
		if err != nil {
			return e.Chain(err, "Migration \"%s\" failed because schema could not be initialized", migrationID)
		}
		if canInitializeSchema {
			if err := g.runInitSchema(); err != nil {
				return e.Chain(err, "Migration \"%s\" failed due to inability to run initSchema", migrationID)
			}
			if err = g.commit(); err != nil {
				return e.Chain(err, "Migration \"%s\" failed", migrationID)
			}

		}
	}

	for i, migration := range g.migrations {
		if err := g.runMigration(migration); err != nil {
			return e.Chain(err, "Migration \"%s\" failed", g.migrations[i].ID)
		}
		if migrationID != "" && migration.ID == migrationID {
			break
		}
	}

	if err := g.commit(); err != nil {
		return e.Chain(err, "Unable to commit migrations")
	}

	return nil
}

// There are migrations to apply if either there's a defined
// initSchema function or if the list of migrations is not empty.
func (g *Gormigrate) hasMigrations() bool {
	return g.initSchema != nil || len(g.migrations) > 0
}

// Check whether any migration is using a reserved ID.
// For now there's only have one reserved ID, but there may be more in the future.
func (g *Gormigrate) checkReservedID() error {
	for _, m := range g.migrations {
		if m.ID == initSchemaMigrationID {
			return e.NewError(`gormigrate: Reserved migration ID: "%s"`, m.ID)
		}
	}
	return nil
}

func (g *Gormigrate) checkDuplicatedID() error {
	lookup := make(map[string]struct{}, len(g.migrations))
	for _, m := range g.migrations {
		if _, ok := lookup[m.ID]; ok {
			return e.NewError(`gormigrate: Duplicated migration ID: "%s"`, m.ID)
		}
		lookup[m.ID] = struct{}{}
	}
	return nil
}

func (g *Gormigrate) checkIDExist(migrationID string) error {
	for _, migrate := range g.migrations {
		if migrate.ID == migrationID {
			return nil
		}
	}
	return ErrMigrationIDDoesNotExist()
}

// RollbackLast undo the last migration
func (g *Gormigrate) RollbackLast() error {
	if len(g.migrations) == 0 {
		return ErrNoMigrationDefined()
	}

	g.begin()
	defer g.rollback()

	lastRunMigration, err := g.getLastRunMigration()
	fn := func(m *Migration) string {
		if m == nil {
			return "<no migration last run>"
		} else {
			return m.ID
		}
	}
	if err != nil {
		return e.Chain(err, "Rollback of \"%s\" failed", fn(lastRunMigration))
	}

	if err := g.rollbackMigration(lastRunMigration); err != nil {
		return e.Chain(err, "Rollback of \"%s\" failed", fn(lastRunMigration))
	}
	if err = g.commit(); err != nil {
		return e.Chain(err, "Rollback of \"%s\" failed", fn(lastRunMigration))
	}
	return nil
}

// RollbackTo undoes migrations up to the given migration that matches the `migrationID`.
// Migration with the matching `migrationID` is not rolled back.
func (g *Gormigrate) RollbackTo(migrationID string) error {
	if len(g.migrations) == 0 {
		return ErrNoMigrationDefined()
	}

	if err := g.checkIDExist(migrationID); err != nil {
		return e.Chain(err, "Cannot rollback to %s", migrationID)
	}

	g.begin()
	defer g.rollback()

	for i := len(g.migrations) - 1; i >= 0; i-- {
		migration := g.migrations[i]
		if migration.ID == migrationID {
			break
		}

		migrationRan, err := g.migrationRan(migration)
		if err != nil {
			return e.Chain(err, "Failed to check if migration \"%s\" ran", migration.ID)
		}

		if migrationRan {
			if err := g.rollbackMigration(migration); err != nil {
				return e.Chain(err, "Failed to rollback %s", migration.ID)
			}
		}
	}
	if err := g.commit(); err != nil {
		return e.Chain(err, "Unable to commit rollback to %s", migrationID)
	}

	return nil
}

func (g *Gormigrate) getLastRunMigration() (*Migration, error) {
	for i := len(g.migrations) - 1; i >= 0; i-- {
		migration := g.migrations[i]

		migrationRan, err := g.migrationRan(migration)
		if err != nil {
			return nil, e.Chain(err, "Failed to check if migration \"%s\" ran", migration.ID)
		}

		if migrationRan {
			return migration, nil
		}
	}
	return nil, ErrNoRunMigration()
}

// RollbackMigration undo a migration.
func (g *Gormigrate) RollbackMigration(m *Migration) error {
	g.begin()
	defer g.rollback()

	if err := g.rollbackMigration(m); err != nil {
		return e.Chain(err, "Unable to rollback of %s!", m.ID)
	}
	if err := g.commit(); err != nil {
		return e.Chain(err, "Unable to commit rollback of %s!", m.ID)
	}

	return nil
}

func (g *Gormigrate) rollbackMigration(m *Migration) error {
	if m.Rollback == nil {
		return ErrRollbackImpossible()
	}

	if err := m.Rollback(g.tx); err != nil {
		return e.Chain(err, "Unable to rollback %s", m.ID)
	}

	sql := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", g.options.TableName, g.options.IDColumnName)
	if err := g.tx.Exec(sql, m.ID).Error; err != nil {
		return e.Chain(err, "Unable to rollback %s", m.ID)
	}

	return nil
}

func (g *Gormigrate) runInitSchema() error {
	if err := g.initSchema(g.tx); err != nil {
		return e.Chain(err, "Unable to run init schema")
	}
	if err := g.insertMigration(initSchemaMigrationID); err != nil {
		return e.Chain(err, "Unable to run init schema migration")
	}

	for i, migration := range g.migrations {
		if err := g.insertMigration(migration.ID); err != nil {
			return e.Chain(err, "Unable to run migration %s", g.migrations[i].ID)
		}
	}

	return nil
}

func (g *Gormigrate) runMigration(migration *Migration) error {
	if len(migration.ID) == 0 {
		return ErrMissingID()
	}

	migrationRan, err := g.migrationRan(migration)
	if err != nil {
		return e.Chain(err, "Unable to run migration %s", migration.ID)
	}
	if !migrationRan {
		if err := migration.Migrate(g.tx); err != nil {
			return e.Chain(err, "Unable to run migration %s", migration.ID)
		}

		if err := g.insertMigration(migration.ID); err != nil {
			return e.Chain(err, "Unable to run migration %s", migration.ID)
		}
	}
	return nil
}

func (g *Gormigrate) createMigrationTableIfNotExists() error {
	if g.tx.HasTable(g.options.TableName) {
		return nil
	}

	sql := fmt.Sprintf("CREATE TABLE %s (%s VARCHAR(%d) PRIMARY KEY)", g.options.TableName, g.options.IDColumnName, g.options.IDColumnSize)
	if err := g.tx.Exec(sql).Error; err != nil {
		return e.Chain(err, "Unable to create migration table with name %s and column %s(size=%d)", g.options.TableName, g.options.IDColumnName, g.options.IDColumnSize)
	} else {
		return nil
	}
}

func (g *Gormigrate) migrationRan(m *Migration) (bool, error) {
	var count int
	err := g.tx.
		Table(g.options.TableName).
		Where(fmt.Sprintf("%s = ?", g.options.IDColumnName), m.ID).
		Count(&count).
		Error
	if err != nil {
		return count > 0, e.Chain(err, "Unable to check if migration \"%s\" ran", m.ID)
	} else {
		return count > 0, err
	}
}

// The schema can be initialised only if it hasn't been initialised yet
// and no other migration has been applied already.
func (g *Gormigrate) canInitializeSchema() (bool, error) {
	migrationRan, err := g.migrationRan(&Migration{ID: initSchemaMigrationID})
	if err != nil {
		return false, e.Chain(err, "Unable to check if migration \"%s\" ran", initSchemaMigrationID)
	}
	if migrationRan {
		return false, nil
	}

	// If the ID doesn't exist, we also want the list of migrations to be empty
	var count int
	err = g.tx.
		Table(g.options.TableName).
		Count(&count).
		Error

	if err != nil {
		return count == 0, e.Chain(err, "Unable to fetch migration count")
	} else {
		return count == 0, nil
	}

}

func (g *Gormigrate) unknownMigrationsHaveHappened() (bool, error) {
	sql := fmt.Sprintf("SELECT %s FROM %s", g.options.IDColumnName, g.options.TableName)
	rows, err := g.tx.Raw(sql).Rows()
	if err != nil {
		return false, e.Chain(err, "Unable to fetch rows for SQL `%s`", sql)
	}
	defer rows.Close()

	validIDSet := make(map[string]struct{}, len(g.migrations)+1)
	validIDSet[initSchemaMigrationID] = struct{}{}
	for _, migration := range g.migrations {
		validIDSet[migration.ID] = struct{}{}
	}

	for rows.Next() {
		var pastMigrationID string
		if err := rows.Scan(&pastMigrationID); err != nil {
			return false, e.Chain(err, "Unable to scan row when checking for unknown migrations")
		}
		if _, ok := validIDSet[pastMigrationID]; !ok {
			return true, nil
		}
	}

	return false, nil
}

func (g *Gormigrate) insertMigration(id string) error {
	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (?)", g.options.TableName, g.options.IDColumnName)
	if err := g.tx.Exec(sql, id).Error; err != nil {
		return e.Chain(err, "Unable to insert migration \"%s\" into DB", id)
	}
	return nil
}

func (g *Gormigrate) begin() {
	if g.options.UseTransaction {
		g.tx = g.db.Begin()
	} else {
		g.tx = g.db
	}
}

func (g *Gormigrate) commit() error {
	if g.options.UseTransaction {
		if err := g.tx.Commit().Error; err != nil {
			return e.Chain(err, "Unable to commit transaction")
		}
	}
	return nil
}

func (g *Gormigrate) rollback() {
	if g.options.UseTransaction {
		g.tx.Rollback()
	}
}
