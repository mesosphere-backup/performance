package systemd

import (
	"strings"

	"github.com/coreos/go-systemd/dbus"
)

// SystemdUnitProps describes a systemd unit entity.
type SystemdUnitProps struct {
	Pid  uint32
	Name string
}

// GetSystemdUnitsProps returns a list of systemd units available on a system.
func GetSystemdUnitsProps() ([]*SystemdUnitProps, error) {
	conn, err := dbus.New()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	uProps := []*SystemdUnitProps{}

	unitStatuses, err := conn.ListUnits()
	if err != nil {
		return nil, err
	}

	for _, unitStatus := range unitStatuses {
		prop, err := conn.GetUnitTypeProperty(unitStatus.Name, "Service", "MainPID")
		if err != nil {
			// skip non service units or the ones lacking MainPID.
			continue
		}

		mainPID, ok := prop.Value.Value().(uint32)
		if !ok {
			// expecting uint32 value for mainPID
			continue
		}

		if mainPID == 0 {
			continue
		}

		if strings.HasPrefix(unitStatus.Name, "sshd@") || strings.HasPrefix(unitStatus.Name, "getty@"){
			continue
		}

		uProps = append(uProps, &SystemdUnitProps{
			Pid:  mainPID,
			Name: unitStatus.Name,
		})
	}
	return uProps, nil
}
