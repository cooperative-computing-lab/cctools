include ../Makefile.config
include ../Makefile.rules

SELECT_SCRIPT = bin/catalog_history_select.py
FILTER_SCRIPT = bin/catalog_history_filter.py
PLOT_SCRIPT = bin/catalog_history_plot.py

test:

all:

clean:

uninstall:
        rm -f ${CCTOOLS_INSTALL_DIR}/${SELECT_SCRIPT}
        rm -f ${CCTOOLS_INSTALL_DIR}/${FILTER_SCRIPT}
        rm -f ${CCTOOLS_INSTALL_DIR}/${PLOT_SCRIPT}

install:
        mkdir -p ${CCTOOLS_INSTALL_DIR}/bin
        chmod 755 ${SELECT_SCRIPT}
        chmod 755 ${FILTER_SCRIPT}
        chmod 755 ${PLOT_SCRIPT}
        cp ${SCRIPT} ${CCTOOLS_INSTALL_DIR}/bin
s
