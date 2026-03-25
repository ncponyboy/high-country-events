import 'dart:convert';
import 'package:flutter/foundation.dart';
import 'package:shared_preferences/shared_preferences.dart';
import '../models/app_region.dart';

class UserPreferencesService extends ChangeNotifier {
  Set<AppRegion> selectedRegions = Set.of(AppRegion.values);

  static const String _regionsKey = 'selectedRegions';

  UserPreferencesService() {
    _load();
  }

  bool get isFilteringByRegion => selectedRegions.length < AppRegion.values.length;

  void toggleRegion(AppRegion region) {
    if (selectedRegions.contains(region)) {
      // Always keep at least one region active
      if (selectedRegions.length <= 1) return;
      selectedRegions = Set.of(selectedRegions)..remove(region);
    } else {
      selectedRegions = Set.of(selectedRegions)..add(region);
    }
    notifyListeners();
    _save();
  }

  Future<void> _load() async {
    try {
      final prefs = await SharedPreferences.getInstance();
      final raw = prefs.getString(_regionsKey);
      if (raw != null) {
        final List<dynamic> decoded = jsonDecode(raw) as List<dynamic>;
        final loaded = decoded
            .whereType<String>()
            .map((rv) {
              try {
                return AppRegion.values.firstWhere((r) => r.rawValue == rv);
              } catch (_) {
                return null;
              }
            })
            .whereType<AppRegion>()
            .toSet();
        if (loaded.isNotEmpty) {
          selectedRegions = loaded;
          notifyListeners();
        }
      }
    } catch (e) {
      debugPrint('UserPreferencesService load error: $e');
    }
  }

  Future<void> _save() async {
    try {
      final prefs = await SharedPreferences.getInstance();
      final raw = jsonEncode(selectedRegions.map((r) => r.rawValue).toList());
      await prefs.setString(_regionsKey, raw);
    } catch (e) {
      debugPrint('UserPreferencesService save error: $e');
    }
  }
}
