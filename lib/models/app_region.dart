import 'package:flutter/material.dart';

enum AppRegion {
  watauga,
  ashe,
  alleghany,
  buncombe,
}

extension AppRegionX on AppRegion {
  String get rawValue {
    switch (this) {
      case AppRegion.watauga:   return 'Watauga County';
      case AppRegion.ashe:      return 'Ashe County';
      case AppRegion.alleghany: return 'Alleghany County';
      case AppRegion.buncombe:  return 'Buncombe County';
    }
  }

  String get shortName {
    switch (this) {
      case AppRegion.watauga:   return 'Boone / Watauga';
      case AppRegion.ashe:      return 'West Jefferson / Ashe';
      case AppRegion.alleghany: return 'Sparta / Alleghany';
      case AppRegion.buncombe:  return 'Asheville / Buncombe';
    }
  }

  IconData get icon {
    switch (this) {
      case AppRegion.watauga:   return Icons.account_balance;
      case AppRegion.ashe:      return Icons.terrain;
      case AppRegion.alleghany: return Icons.eco;
      case AppRegion.buncombe:  return Icons.location_city;
    }
  }

  Color get color {
    switch (this) {
      case AppRegion.watauga:   return Colors.blue;
      case AppRegion.ashe:      return Colors.green;
      case AppRegion.alleghany: return Colors.orange;
      case AppRegion.buncombe:  return Colors.purple;
    }
  }

  List<String> get locationKeywords {
    switch (this) {
      case AppRegion.watauga:
        return [
          'watauga', 'boone', 'blowing rock', 'banner elk', 'valle crucis',
          'vilas', 'zionville', 'sugar grove', 'beech mountain', 'seven devils',
          'deep gap', 'newland', 'meat camp',
        ];
      case AppRegion.ashe:
        return [
          'ashe county', 'west jefferson', 'jefferson, nc', ' jefferson,',
          'lansing', 'creston', 'grassy creek', 'warrensville', 'fleetwood',
          'nathans creek', 'elk cross',
        ];
      case AppRegion.alleghany:
        return [
          'alleghany', 'sparta', 'piney creek', 'glade valley',
          'whitehead', 'roaring gap',
        ];
      case AppRegion.buncombe:
        return [
          'buncombe', 'asheville', 'weaverville', 'black mountain', 'swannanoa',
          'woodfin', 'enka', 'candler', 'arden', 'leicester',
          'fairview, nc', ' fairview,',
        ];
    }
  }

  bool matches(String location) {
    final lower = location.toLowerCase();
    return locationKeywords.any((kw) => lower.contains(kw));
  }

  /// Infer a region from a location string. Returns null if no county matched.
  static AppRegion? infer(String location) {
    for (final region in AppRegion.values) {
      if (region.matches(location)) return region;
    }
    return null;
  }
}
