regression_name = 'KernelRidgeRegression';
regression_accuracy = 1e-08;
init_random = 42;
kernel_feature_type = 'Real';
regression_num_threads = 1;
regression_labels = [1, 1, 1, -1, -1, 1, -1, 1, -1, 1, -1];
kernel_arg0_width = 1.5;
kernel_data_train = [0.3745401, 0.9507143, 0.7319939, 0.5986585, 0.1560186, 0.1559945, 0.0580836, 0.8661761, 0.601115, 0.7080726, 0.0205845;0.9699099, 0.8324426, 0.2123391, 0.181825, 0.1834045, 0.3042422, 0.5247564, 0.431945, 0.2912291, 0.6118529, 0.1394939;0.2921446, 0.3663618, 0.45607, 0.785176, 0.1996738, 0.5142344, 0.5924146, 0.0464504, 0.6075449, 0.1705241, 0.0650516;0.9488855, 0.965632, 0.8083973, 0.3046138, 0.0976721, 0.684233, 0.4401525, 0.1220382, 0.4951769, 0.0343885, 0.9093204;0.25878, 0.6625223, 0.3117111, 0.520068, 0.5467103, 0.1848545, 0.9695846, 0.7751328, 0.9394989, 0.8948274, 0.5979;0.9218742, 0.0884925, 0.1959829, 0.0452273, 0.3253303, 0.3886773, 0.271349, 0.8287375, 0.3567533, 0.2809345, 0.5426961;0.1409242, 0.802197, 0.0745506, 0.9868869, 0.7722448, 0.1987157, 0.0055221, 0.8154614, 0.7068573, 0.7290072, 0.7712703;0.0740447, 0.3584657, 0.1158691, 0.8631034, 0.6232981, 0.330898, 0.0635584, 0.3109823, 0.3251833, 0.7296062, 0.6375575;0.8872127, 0.4722149, 0.1195942, 0.7132448, 0.760785, 0.5612772, 0.7709672, 0.4937956, 0.5227328, 0.427541, 0.0254191;0.1078914, 0.0314292, 0.6364104, 0.314356, 0.5085707, 0.9075665, 0.2492922, 0.4103829, 0.7555511, 0.2287982, 0.0769799;0.2897515, 0.1612213, 0.9296977, 0.8081204, 0.6334038, 0.8714606, 0.8036721, 0.1865701, 0.892559, 0.5393422, 0.8074402];
kernel_name = 'Gaussian';
regression_type = 'kernelmachine';
regression_tau = 1e-05;
kernel_feature_class = 'simple';
kernel_data_test = [0.8960913, 0.3180035, 0.1100519, 0.2279352, 0.4271078, 0.8180148, 0.8607306, 0.0069521, 0.5107473, 0.417411, 0.2221078, 0.1198654, 0.3376152, 0.9429097, 0.3232029, 0.5187906, 0.703019;0.3636296, 0.9717821, 0.9624473, 0.2517823, 0.4972485, 0.3008783, 0.2848405, 0.0368869, 0.6095643, 0.502679, 0.0514788, 0.2786465, 0.9082659, 0.2395619, 0.1448949, 0.4894528, 0.9856505;0.2420553, 0.6721355, 0.7616196, 0.2376375, 0.7282163, 0.3677831, 0.6323058, 0.6335297, 0.5357747, 0.0902898, 0.8353025, 0.3207801, 0.1865185, 0.0407751, 0.5908929, 0.6775644, 0.0165878;0.5120931, 0.2264958, 0.6451728, 0.1743664, 0.6909377, 0.3867353, 0.93673, 0.1375209, 0.3410664, 0.1134735, 0.9246936, 0.8773394, 0.2579416, 0.659984, 0.8172222, 0.5552008, 0.5296506;0.2418523, 0.0931028, 0.8972158, 0.9004181, 0.6331015, 0.3390298, 0.3492096, 0.7259557, 0.8971103, 0.8870864, 0.7798755, 0.6420316, 0.08414, 0.1616287, 0.8985542, 0.6064291, 0.0091971;0.1014715, 0.6635018, 0.0050616, 0.1608081, 0.5487338, 0.6918952, 0.6519613, 0.2242693, 0.7121792, 0.2372491, 0.3253997, 0.7464914, 0.6496329, 0.8492234, 0.6576129, 0.5683086, 0.0936748;0.3677158, 0.2652024, 0.2439896, 0.9730106, 0.3930977, 0.8920466, 0.6311386, 0.7948113, 0.5026371, 0.5769039, 0.4925177, 0.195243, 0.7224521, 0.2807724, 0.024316, 0.6454723, 0.1771107;0.9404586, 0.9539286, 0.9148644, 0.3701587, 0.0154566, 0.9283186, 0.4281841, 0.9666548, 0.96362, 0.8530095, 0.2944489, 0.3850977, 0.8511367, 0.316922, 0.1694927, 0.5568013, 0.9361548;0.6960298, 0.5700612, 0.0971765, 0.6150072, 0.9900539, 0.140084, 0.5183297, 0.8773731, 0.7407686, 0.6970157, 0.7024841, 0.3594912, 0.2935918, 0.8093612, 0.8101134, 0.8670723, 0.9132406;0.5113424, 0.5015163, 0.7982952, 0.6499639, 0.7019669, 0.7957927, 0.8900053, 0.3379952, 0.375583, 0.0939819, 0.5782801, 0.0359423, 0.465598, 0.5426446, 0.2865413, 0.5908333, 0.0305002;0.0373482, 0.8226006, 0.3601906, 0.1270605, 0.5222433, 0.7699936, 0.215821, 0.6228905, 0.0853475, 0.0516817, 0.5313546, 0.5406351, 0.6374299, 0.7260913, 0.9758521, 0.5163003, 0.3229565];
regression_classified = [0.717148566, 0.524856899, 0.14568107, -0.49590241, -0.052540131, 0.107256851, 0.558035852, -1.34632579, 0.439553508, 0.640874104, -0.703996365, -0.186053944, 0.583634904, 0.812115714, -0.604674472, -0.233305351, 0.744808945];
