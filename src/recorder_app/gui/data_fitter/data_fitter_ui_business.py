import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
from scipy.optimize import curve_fit, differential_evolution, brute

from recorder_app.infrastructure.handler.hydride_handler import MetalHydrideDatabase

from recorder_app.config_connection_reading_management.database_reading_writing import DataRetriever
from recorder_app.infrastructure.core.table_config import TableConfig
try:
    import recorder_app.infrastructure.core.logger as logging
except ImportError:
    import logging

standard_constraints_dict = {
            "min_TotalCharTime": 0.33,
            "max_TotalCharTime": 1,
            "min_TotalTempIncr": 2,
            "max_TotalTempIncr": 5
                            }

def remove_order_term(input_string):
    keyword = "order"
    index = input_string.lower().find(keyword)
    if index != -1:
        # Slice the string up to the index where "ORDER" starts
        return input_string[:index]
    else:
        # "ORDER" not found; return the original string
        return input_string


class DataLoader:
    def __init__(self, file_path=None, sample_id=None, cycle_number=None, temperature=None, db_conn_params=None):
        self.logger = logging.getLogger(__name__)
        self.file_path = file_path
        self.db_conn_params = db_conn_params or {}
        self.data_retriever = DataRetriever(db_conn_params=self.db_conn_params)
        self.standard_constraints_dict = standard_constraints_dict
        self.etc_table = TableConfig().ETCDataTable
        self.sample_id = sample_id
        self.temperature = temperature
        self.cycle_number = cycle_number

    def read_data(self):
        if self.file_path:
            return pd.read_csv(self.file_path, delimiter=',')
        else:
            return None

    def get_isotherm(self, sample_id=None, cycle_number=None, temperature=None):
        """

        :param sample_id:
        :param cycle_number:
        :param temperature:
        :return: isotherm:
                 mean_temperature:
                 de_hyd_state:
        """

        isotherm = self._read_isotherm(sample_id=sample_id, cycle_number=cycle_number, temperature=temperature)
        mean_temperature, de_hyd_state = self._process_isotherm(isotherm)
        return isotherm, mean_temperature, de_hyd_state

    def _read_isotherm(self, sample_id=None, cycle_number=None, temperature=None):
        sample_id = sample_id if sample_id else self.sample_id
        cycle_number = cycle_number if cycle_number else self.cycle_number
        temperature = temperature if temperature else self.temperature


        column_names = (self.etc_table.temperature_sample, self.etc_table.pressure,
                        self.etc_table.th_conductivity, self.etc_table.de_hyd_state)
        query, values = self.data_retriever.qb.create_reading_query(table_name=self.etc_table.table_name,
                                                            column_names=column_names,
                                                            constraints=self.standard_constraints_dict)
        query = remove_order_term(query)
        query += f" AND {self.etc_table.sample_id_small} = %s "
        query += f" AND {self.etc_table.cycle_number} = %s "
        query += f" AND {self.etc_table.temperature} = %s "
        query += f" ORDER by {self.etc_table.pressure}"
        values += (sample_id, cycle_number, temperature)
        #print(query, values)
        results = self.data_retriever.execute_fetching(query=query, column_names=column_names, values=values)
        return results

    def _process_isotherm(self, df_isotherm):
        if df_isotherm.empty:
            return None, None
        mean_temperature = np.mean(df_isotherm[self.etc_table.temperature_sample])
        if df_isotherm[self.etc_table.de_hyd_state].iloc[0] == df_isotherm[self.etc_table.de_hyd_state].iloc[-1]:
            de_hyd_state = df_isotherm[self.etc_table.de_hyd_state].iloc[0]
        else:
            self.logger.info("No consistent de_hyd_state")
            de_hyd_state = None
        return mean_temperature, de_hyd_state

    @staticmethod
    def write_data():
        example_data = """Druck\tThAverage\tSampleTemp\tTh_Conductivity
                            0.1\t0.02\t30\t0.018
                            0.2\t0.025\t30\t0.019
                            0.3\t0.028\t30\t0.0185
                            0.4\t0.032\t30\t0.0195
                            0.5\t0.03\t30\t0.020
                            1.0\t0.035\t30\t0.022
                            2.0\t0.04\t30\t0.024
                            3.0\t0.038\t30\t0.023
                            5.0\t0.045\t30\t0.026
                            7.0\t0.048\t30\t0.027
                            10.0\t0.05\t30\t0.028
                            20.0\t0.055\t30\t0.030
                            """
        with open("gui/data_fitter/example_data.txt", "w") as file:
            file.write(example_data)


class MaterialProperties:
    # models stolen from: International Journal of Heat and Mass Transfer 187 (2022) 122519
    #Sebastian Sonnick, Lars Erlbeck,  Manuel Meier,  Hermann Nirschl,  Matthias Rädle

    def __init__(self, sample_mass=18.0741e-3, cell_type='3rd', cell_volume=None,
                 material=None, de_hyd_state="Dehydrogenated", particle_diameter=45e-6):
        if not material:
            return
        self.logger = logging.getLogger(__name__)
        self.hydride_worker = MetalHydrideDatabase(db_conn_params=config_reader.config.db_conn_params)
        self.material = material # material name MgH2
        self.particle_diameter = particle_diameter

        ##constants
        self.gas = 'H2' # gas name Hydrogen
        self.lambda_null_gas = 0.018    # Conductivity H2 W/m/K
        self.kB = 1.3806e-23   # k Boltzmann m^2 kg s^-2 K^-1
        self.sigma0 = 0.27e-18 # sigma0 = d_collision^2*pi
        Cp = 14.199 #J/kg/K http://www.h2data.de/
        Cv = 10.074 #J/kg/K  http://www.h2data.de/

        self.kappa = Cp/Cv
        #Pr =   0.69        ;   %Prandtl number 0.69 NPr at 100 °C H2 https://pdf.sciencedirectassets.com/286836/3-s2.0-C20100666909/3-s2.0-B9780444538048000071/main.pdf?X-Amz-Security-Token=IQoJb3JpZ2luX2VjECAaCXVzLWVhc3QtMSJGMEQCIGdxUtGkzc3hmZQFjo4Yr0akMbbgap2OXjnlS%2BBY5gplAiBPnwrzU2b75QS75ERf9la9e0L00pswz%2BZUdSFIXIKLdCq8BQiJ%2F%2F%2F%2F%2F%2F%2F%2F%2F%2F8BEAUaDDA1OTAwMzU0Njg2NSIMeN3hsLHqlnG9NdalKpAFk9ZsDE7qju%2BXArSzaNnHA0Wa9BBfvPkTNRGGK1kNOE1KOlV4DbJPfvfea15hx2VTtX67X0d30CBa2YgxryYws6wndGGiVaVCgrg98ibAVo6RMX9kLFyDiyHUo%2BZ%2B7CuQwJ5nnUuRx6ThEeOCuAtI5yKVMofa3Z6Tzn4nGvT5qU3pJphR9Z14iOej4sgWPXnwmLVROvUYUcDgL3ycx%2F29Agqw57kJFgnKWHSZfu9ESitQo%2BJMr9DnMSYIYd%2BNZ6gQj9%2BYTULYOnAxVI6t0pASKadWMUo6nY07%2F7AcKuqaqT2cjjzdEuY3pfh%2BCDz%2FGukP9U2vVEOwG%2BF0MdRXHFgLnAU8AU4ROHk%2Bz%2B24zaU6vVwwiOdoZwH2XCPysKeyGvoA7Oq5%2BPduojQ%2FbjUpSvatD%2BvLyFNklYPMuUYvVCeQs65ceiMzYu30vJ6SUWInUhv%2FgWkVLXHgofxhLgevfB6YyueZqvP87XNIEjVWV8sXNEs3r7DIq16nYQ%2BWzgnFGSHvFC4eFZ5yjvlA009QyuHXnTbe2DLA5rGsyzefScv3ZX9plAaP65TUMhD8IP9miywolKAsTpO1qN3HZcJId0grZvrzGQqgmzKoE6dRPPbXYJNYDvCNiUNEco3gdxgYGQpAfXQSJ%2BcMvR1rKtoXPDouDyoJ5gY44C6n11CdlshY5ANni%2BA1MbkhNNjT1yL3yLBRAFIQc6UuPJuBGAVWlV79BVFCD4f8q5Yf%2BKIre2SHlc1u3Vc5lonXU8UwjF39Jjvy5lxvNBI56sid%2BA0eCcmUDOadMP0x8IKV%2FG26JUajlO1VsntV9QZSm1RvJbtwlcAFl2G4XkpfP5IfEZix72IbHbYb7BB7Zbw2iNveDj8V7AUw4uz0pAY6sgGRRsTSShOceOs8WLY5YTbAbTc4HT3v2cpkfOo3%2Fdm1hghRVjE2JbL7c4jKHgynysZi6k2VJ14T0u7kf3tweCNh%2B25gUC66A7b3McP6cZn3AhVPzw6J3SjRieN2aj1e2mclpA52Zo19ZXlMTi1Ldrk3a5c%2FxCT5cxRNtA3rT1PTINYYW7Pe269UDr3TCN73%2F4cAOgHUoM0Yi%2F5Un%2FxYq19AW0CuMMEMe7Ww5SuH5%2FALdigd&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230629T080939Z&X-Amz-SignedHeaders=host&X-Amz-Expires=300&X-Amz-Credential=ASIAQ3PHCVTY6XPCCIGE%2F20230629%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=f16d8d2fbd8d41407eef8c751a7753124df38023356389eca4a7080429d54869&hash=e67e749f67bc14e9fe309deafa74c7dbc5239a7dcee04ffa5d02ef34b165c4ef&host=68042c943591013ac2b2430a89b270f6af2c76d8dfd086a07176afe7c76c2c61&pii=B9780444538048000071&tid=spdf-14d08327-b054-4d58-b676-2c98b2e6d1a9&sid=a950bce3220d444c135b9ff8eb2c8247c234gxrqb&type=client&tsoh=d3d3LnNjaWVuY2VkaXJlY3QuY29t&ua=1e0650050502075f5657&rr=7decb0452e5e2c75&cc=de
        self.Pr = 4 * self.kappa / (9 * self.kappa - 5) # Prandtl number: International Journal of Heat and Mass Transfer 187 (2022) 122519


        self.sample_mass = self.get_mass(material=self.material, sample_mass=sample_mass) #g
        self.volume_cell = self.get_cell_volume(cell_volume=cell_volume, cell_type=cell_type)

        self.molar_mass_gas = self.get_molar_mass(material=self.gas)
        self.molar_mass_material = self.get_molar_mass(material=self.material)
        self.density_material = self.get_density(material=self.material)
        self.porosity = 1 - ((self.sample_mass) / self.volume_cell / self.density_material)
        self.lambda_solid = self.get_initial_conductivity(material=self.material, de_hyd_state=de_hyd_state)

        self.alpha_values = self.alpha_funs(self.molar_mass_gas, self.molar_mass_material)
        #print(self.alpha_values)
        #print(f"Amount alpha valse {len(self.alpha_values)}")
        self.beta_values = [self.beta_funs(alpha=value, name=name) for name, value in self.alpha_values.items()]
        #print(self.beta_values)
        #print(f"Amount beta valse {len(self.beta_values)}")

    def get_cell_volume(self, cell_volume=None, cell_type=None):
        if cell_volume:
            volume_cell = cell_volume # m^3
        elif cell_type == '3rd':
            volume_cell = 40.37 * 1e-6 # m^3
        elif cell_type == '2nd':
            volume_cell = 32 * 1e-6 # m^3
        else:
            volume_cell = None
            self.logger.infor("No cell volume found")
        return volume_cell

    def get_molar_mass(self, material):
        return self.hydride_worker.get_molar_mass_hydride(hydride_name=material)  # g/mol

    def get_density(self, material):
        return self.hydride_worker.get_density(hydride_name=material) * 1e3 #kg/m

    def get_mass(self, material, sample_mass):
        capacity = self.hydride_worker.get_capacity(hydride_name=material)
        if capacity:
            return sample_mass + sample_mass * capacity/100
        else:
            return sample_mass

    def get_initial_conductivity(self, material, de_hyd_state):
        conductivity = self.hydride_worker.get_bulk_conductivity(hydride_name=material, state=de_hyd_state)
        return conductivity

    def alpha_funs(self, M_gas, M_solid):
        alpha_baule = 2 * M_gas * M_solid / (M_gas + M_solid)**2
        alpha_goodman = 2.4 * (M_gas / M_solid) / (1 + (M_gas / M_solid))**2
        alpha_kaganer = 1 - ((M_solid - M_gas) / (M_solid + M_gas))**2
        alpha_bauer = 1 - 144.6 / ((M_gas + 12)**2)
        return {"alpha_baule": alpha_baule, "alpha_goodman": alpha_goodman,
                "alpha_kaganer": alpha_kaganer, "alpha_bauer": alpha_bauer}

    def beta_funs(self, alpha, name):
        if alpha>0:
            beta1 = (2 - alpha) / alpha * 2 * self.kappa / (self.kappa + 1) * 1 / self.Pr
            beta2 = 0.5 * (2 - alpha) / alpha * 2 * self.kappa / (self.kappa + 1) * 1 / self.Pr
            beta3 = (2 - alpha) / alpha
            beta4 = (5 * np.pi / 32) * (9 * self.kappa - 5) / (self.kappa + 1) * (2 - alpha) / alpha
            return {"beta1_"+name: beta1, "beta2_"+name:beta2,
                    "beta3_"+name:beta3, "beta4_"+name:beta4}
        else:
            return 0


class BaseModel:
    def __init__(self, material_properties=MaterialProperties(), Temperature=None, beta=None, beta_name=None):
        self.mp = material_properties
        self.lambda_bulk = self.mp.lambda_solid
        self.T = Temperature + 273.15
        self.beta = beta

    def char_length_fun (self, porosity, particle_diameter):
        return (2/3) * (porosity / (1 - porosity)) * particle_diameter

    def lmfw(self, p): #mean free path [m]
        return self.mp.kB * self.T / (np.sqrt(2) * self.mp.sigma0 * p)   #kb T/(sqrt(2) pi * d^2 * p)

    def lambda_gas_fun(self, p, porosity, particle_diameter, lambda_null_gas, beta):
        knudsen_number = self.lmfw(p) / self.char_length_fun(porosity, particle_diameter)
        return lambda_null_gas / (1 + 2 * beta * knudsen_number)

    def kapa_kaganer(self, p, porosity, particle_diameter, lambda_null_gas, beta, lambda_particle):
        return 1 - self.lambda_gas_fun(p, porosity, particle_diameter, lambda_null_gas, beta) / lambda_particle

    #model


class KaganerModel(BaseModel):

    def __init__(self, material_properties=MaterialProperties(),
                         Temperature=None,
                         beta=None,
                         beta_name=None):
        super().__init__(material_properties=material_properties,
                         Temperature=Temperature,
                         beta=beta,
                         beta_name=beta_name)

    def ETC_fun(self, p, params):
        particle_diameter, lambda_null_gas, porosity, lambda_base = params
        lambda_gas = self.lambda_gas_fun(p, porosity, particle_diameter, lambda_null_gas, self.beta)
        k_g = self.kapa_kaganer(p, porosity, particle_diameter, lambda_null_gas, self.beta, self.lambda_bulk)
        #print(lambda_gas, self.lambda_bulk)
        return lambda_base + (lambda_gas * (5.81 * ((1 - porosity)**2) / k_g) *
                         (1 / k_g * np.log(self.lambda_bulk / lambda_gas) - 1 - (k_g / 2)) + 1)


class ZehnerBauerSchluenderModel(BaseModel):

    def __init__(self, material_properties=MaterialProperties(),
                         Temperature=None,
                         beta=None,
                         beta_name=None):
        super().__init__(material_properties=material_properties,
                         Temperature=Temperature,
                         beta=beta,
                         beta_name=beta_name)

    #model
    def ETC_fun(self, p, params):
        def b(porosity):
            c = 1.25
            return c * ((1 - porosity) / porosity)**(10/9)

        particle_diameter, lambda_null_gas, porosity, lambda_base = params

        l_g = self.lambda_gas_fun(p, porosity, particle_diameter, lambda_null_gas, self.beta)
        k_g = self.kapa_kaganer(p, porosity, particle_diameter, lambda_null_gas, self.beta, self.lambda_bulk)
        term1 = (1 - np.sqrt(1 - porosity))
        term2 = (2 * np.sqrt(1 - porosity) / (k_g * b(porosity)))
        term3 = (k_g * b(porosity) / (1 - l_g / self.lambda_bulk)**2 * np.log(self.lambda_bulk / (l_g * b(porosity))))
        term4 = ((b(porosity) + 1) / 2)
        term5 = ((b(porosity) - 1) / (k_g * b(porosity)))
        return lambda_base + l_g * (term1 + term2 * (term3 - term4 - term5))


class ModelFitter:
    def __init__(self, model):
        self.model = model
        self.x0 = self.create_starting_values()
        self._fit_methods = {
            'curve_fit': self._fit_curve_fit,
            'differential_evolution': self._fit_differential_evolution,
            'brute': self._fit_brute,
            'curve_fit_log': self._fit_curve_fit_log
        }

    def create_starting_values(self):
        return [self.model.mp.particle_diameter,
          self.model.mp.lambda_solid, self.model.mp.porosity, 1e-5]

    def fit_ETC(self, x, y, method='curve_fit', **kwargs):
        if method not in self._fit_methods:
            raise ValueError(f"Unknown fitting method: {method}")
        params, pcov = self._fit_methods[method](x, y, **kwargs)
        if 'log' in method:
            metrics = self.calculate_metrics(x, y, params, log_space=True)
        else:
            metrics = self.calculate_metrics(x, y, params)

        return params, pcov, metrics

    def _fit_curve_fit(self, x, y, bounds):
        def fit_function(x, params):
            d, lambda_null_gas, porosity, lambda_base = params
            return self.model.ETC_fun(x, params)

        if bounds:
            popt, pcov = curve_fit(lambda x, d, lambda_null_gas, porosity, lambda_base:
                                   fit_function(x, (d, lambda_null_gas, porosity, lambda_base)),
                                   x, y,  p0=self.x0, bounds=bounds)
        else:
            popt, pcov = curve_fit(lambda x, d, lambda_null_gas, porosity, lambda_base:
                                   fit_function(x, (d, lambda_null_gas, porosity, lambda_base)),
                                   x, y,  p0=self.x0)

        return popt, pcov

    def _fit_differential_evolution(self, x, y, bounds):
        def fit_function(params, x, y):
            #d, lambda_null_gas, porosity, lambda_base = params
            predicted_y = self.model.ETC_fun(x, params)
            return np.sum((y - predicted_y) ** 2)

        result = differential_evolution(fit_function, bounds, args=(x, y))
        return result.x, result.fun

    def _fit_brute(self, x, y, ranges):
        def fit_function_zbs(params, x, y):
            # d, lambda_null_gas, porosity, lambda_base = params
            predicted_y = self.model.ETC_fun(x, params)
            return np.sum((y - predicted_y) ** 2)

        result = brute(fit_function_zbs, ranges=ranges, args=(x, y), full_output=True, finish=None)
        best_params = result[0]
        best_score = result[1]
        return best_params, best_score

    def _fit_curve_fit_log(self, x, y, bounds=None):
        # Log-transform the data
        x_log = np.log10(x)
        y_log = np.log10(y)

        # Define the fitting function in log-log space
        def fit_function(x_log, d, lambda_null_gas, porosity, lambda_base):
            params = (d, lambda_null_gas, porosity, lambda_base)
            # Return log10 of the model output
            return np.log10(self.model.ETC_fun(10**x_log, params))

        if bounds:
            popt, pcov = curve_fit(
                fit_function,
                x_log,
                y_log,
                p0=self.x0,
                bounds=bounds
            )
        else:
            popt, pcov = curve_fit(
                fit_function,
                x_log,
                y_log,
                p0=self.x0
            )
        return popt, pcov

    def calculate_metrics(self, x, y, params, log_space=False):
        if log_space:
            y_pred = self.model.ETC_fun(x, params)
            residuals = np.log10(y) - np.log10(y_pred)
        else:
            y_pred = self.model.ETC_fun(x, params)
            residuals = y - y_pred

        ss_res = np.sum(residuals ** 2)
        if log_space:
            ss_tot = np.sum((np.log10(y) - np.mean(np.log10(y))) ** 2)
        else:
            ss_tot = np.sum((y - np.mean(y)) ** 2)
        r_squared = 1 - (ss_res / ss_tot)
        rmse = np.sqrt(np.mean(residuals ** 2))
        mae = np.mean(np.abs(residuals))
        return {'R_squared': r_squared, 'RMSE': rmse, 'MAE': mae}

class Plotter:
    @staticmethod
    def plot_results(x, y, fitted_values_dict, x_plot):
        plt.figure()
        plt.plot(x, y, '-x', label='Measured Data')

        for beta_name, fitted_values in fitted_values_dict.items():
            plt.plot(x_plot, fitted_values, label=f'Fitted Data {beta_name}')

        plt.xscale('log')
        plt.xlabel('Pressure (bar)')
        plt.ylabel('Thermal Conductivity (W/m/K)')
        plt.legend()
        plt.title('ETC Fit for Different Beta Values')
        plt.show()


def main(mode='curve_fit', data_loader=None):
    #todo: Benzin drauf, abfackeln, 10 Ave Maria und dann vielleicht alles von vorn
    # oder einfach verschweigen dass ich das je versucht habe.
    #file_path = r"C:\Daten\Kiki\WAE-WA-028-MgFe3wt\Results\Results-WAE-WA-028-044\WAE-WA-028-044-AllData.txt"
    # Load data
    from recorder_app.infrastructure.core.config_reader import config

    if not data_loader:
        data_loader = DataLoader(sample_id="WAE-WA-040", cycle_number=0.5, temperature=200, db_conn_params=config.db_conn_params)
    isotherm, mean_temperature, de_hyd_state = data_loader.get_isotherm()

    #[Particle_Diameter,lambda_solid(1,mat_it),Porosity(mat_it), l_base]
    material_properties = MaterialProperties(material="MgH2", de_hyd_state=de_hyd_state)
    lb = [1e-8, 1e-6, 1e-8, 1e-8]
    ub = [10, 156, 1, 10]
    bounds = None
    bounds_differential = [(1e-8, 10), (1e-8, 10), (1e-8, 1), (1e-8, 10)]
    ranges = [
                (0, 10, 1),  # Example range for particle_diameter with larger step
                (0, 10, 1),  # Example range for lambda_null_gas with larger step
                (0, 1, 0.1),  # Example range for porosity with larger step
                (0, 10, 1)
            ]

    T = mean_temperature
    x = isotherm["pressure"]
    y = isotherm["ThConductivity"]
    # Material properties

    fitted_values_dict = {}
    popt_dict = {}
    pcov_dict = {}
    p_deviation = {}
    x_plot = np.logspace(-5, 4, 100)

    for dict in material_properties.beta_values:
        for name, value in dict.items():
            beta_val = value
            beta_name = name

            model = KaganerModel(material_properties, Temperature=T, beta=beta_val)
            model_fitter = ModelFitter(model)

            try:
                #best_params, best_score = model_fitter.fit_ETC_ZBS_brute(x, y, ranges)
                #popt, pcov = best_params, best_score
                #fitted_values = model.ETC_fun_Zehner_Bauer_Schlunder(x_plot, popt)
                #popt, pcov = model_fitter.fit_ETC_kaganer(x, y, bounds)
                #fitted_values = model.ETC_fun_kaganer(x_plot, popt)
                popt, pcov, metrics = model_fitter.fit_ETC(x=x, y=y, method=mode, bounds=bounds)
                fitted_values = model.ETC_fun(x_plot, popt)

                # Fit using differential evolution
                #opt_params, opt_value = model_fitter.fit_ETC_kaganer_diff(x, y, bounds_differential)
                #fitted_values = model.ETC_fun_kaganer(x_plot, opt_params)
                #pcov = opt_params
                #popt = opt_params
               # print("Optimized parameters using differential evolution:", opt_params)
                #popt_dict[beta_name] = opt_params
                popt_dict[beta_name] = popt
                pcov_dict[beta_name] = pcov
                p_deviation[beta_name] = np.sqrt(np.diag(pcov))
                fitted_values_dict[beta_name] = fitted_values
                print(f"Fitted {beta_name}")
            except Exception as e:
                print(f"nope {e}")
                continue
            for key, value in metrics.items():
                print(f"{key}: {value}")


    print("Optimized parameters for different beta values:")
    for (beta_name, optimized), (_, deviation) in zip(popt_dict.items(), p_deviation.items()):
        print(f"{beta_name}: diameter:  {round(optimized[0], 4)} +- {np.round(deviation[0], 4)} [m]",
                             f"lambda_gas: {round(optimized[1], 4)} +- {np.round(deviation[1], 4)}  [W/m/K]",
                             f"porosity: {round(optimized[2], 4)} +- {np.round(deviation[2], 4)} ",
                            sep="\n")




    Plotter.plot_results(x, y, fitted_values_dict, x_plot)


if __name__ == "__main__":
   main('curve_fit')
   main('curve_fit_log')
