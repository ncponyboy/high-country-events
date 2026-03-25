import 'dart:convert';
import 'package:flutter/foundation.dart';
import 'package:http/http.dart' as http;
import '../models/restaurant_special.dart';

class SpecialsService extends ChangeNotifier {
  List<RestaurantSpecial> specials = [];
  bool isLoading = false;
  String? errorMessage;

  static const String _url =
      'https://raw.githubusercontent.com/ncponyboy/high-country-events-specials/main/specials.json';

  Future<void> fetchSpecials() async {
    isLoading = true;
    errorMessage = null;
    notifyListeners();

    try {
      final response = await http.get(Uri.parse(_url)).timeout(
        const Duration(seconds: 15),
      );

      if (response.statusCode == 200) {
        final data = jsonDecode(response.body) as Map<String, dynamic>;
        final parsed = RestaurantSpecialsResponse.fromJson(data);
        specials = parsed.specials
          ..sort((a, b) => a.priority.compareTo(b.priority));
        errorMessage = null;
      } else {
        errorMessage = 'Failed to load specials (${response.statusCode}).';
      }
    } catch (e) {
      errorMessage = 'Could not load specials.';
      debugPrint('SpecialsService error: $e');
    } finally {
      isLoading = false;
      notifyListeners();
    }
  }

  List<RestaurantSpecial> get activeSpecials =>
      specials.where((s) => s.isActive).toList();
}
